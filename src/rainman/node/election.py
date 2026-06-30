"""Leader election — heartbeat timeout, candidate logic, vote grant.

DDIA Chapter 5 — Replication: Leader election and failover.
Simplified fixed-priority fallback: any follower calls an election when
its randomised timeout expires without receiving a heartbeat.  Vote-grant
conditions ensure only a sufficiently up-to-date candidate wins.
"""

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable

import httpx

logger = logging.getLogger(__name__)

# Maximum wait for vote responses before declaring the election lost.
# DDIA §5.2: a candidate that receives no majority within a bounded
# window resets its timeout and retries rather than blocking forever.
_DEFAULT_VOTE_TIMEOUT_MS = 300


class ElectionManager:
    """Drives leader election for a single cluster node.

    Tracks voted_for per term and runs the background election timeout
    loop.  Does not own the authoritative term, role, or LSN — those
    live in main.py and are accessed through the injected callbacks so
    this class can be unit-tested without FastAPI.

    Raises nothing on construction.  Internal background tasks absorb
    all exceptions and log them; the node keeps running even if an
    individual election attempt fails due to a network error.
    """

    def __init__(
        self,
        node_id: str,
        peers: list[dict],
        replication_cfg: dict,
        get_term: Callable[[], int],
        set_term: Callable[[int], None],
        get_lsn: Callable[[], int],
        get_role: Callable[[], str],
        on_became_leader: Callable[[int], Awaitable[None]],
    ) -> None:
        """Initialise with node context and state-transition callbacks.

        node_id: this node's identifier string.
        peers: list of peer node descriptors (node_id, host, port).
        replication_cfg: the 'replication' block from cluster.json.
        get_term / set_term: read and write the authoritative current
          term held in main.py.
        get_lsn: read the current LSN from the storage engine.
        get_role: read the current role ("leader" | "follower") from
          main.py.
        on_became_leader(new_term): async callback invoked when this
          node wins an election; responsible for updating role/term and
          starting the heartbeat loop.
        """
        self._node_id = node_id
        self._peers = peers
        self._cfg = replication_cfg
        self._get_term = get_term
        self._set_term = set_term
        self._get_lsn = get_lsn
        self._get_role = get_role
        self._on_became_leader = on_became_leader

        # voted_for[term] = candidate_id this node voted for in that term.
        # DDIA §5: voted_for must not be reset between elections in the
        # same run so a node never votes twice in the same term.
        self._voted_for: dict[int, str] = {}

        # Set by reset_timeout() each time a heartbeat or granted vote
        # arrives, clearing the countdown in _timeout_loop.
        self._heartbeat_event: asyncio.Event = asyncio.Event()
        self._timeout_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background election timeout loop.

        Must be called from within a running asyncio event loop (e.g.
        from the FastAPI lifespan).  Idempotent — safe to call again if
        the task is already running.
        """
        if self._timeout_task is None or self._timeout_task.done():
            self._timeout_task = asyncio.create_task(self._timeout_loop())
            logger.debug(
                "Node %s election timeout loop started", self._node_id
            )

    async def stop(self) -> None:
        """Cancel and await the background timeout loop.

        Idempotent — safe to call when already stopped.
        """
        if self._timeout_task and not self._timeout_task.done():
            self._timeout_task.cancel()
            try:
                await self._timeout_task
            except asyncio.CancelledError:
                pass
            self._timeout_task = None

    # ------------------------------------------------------------------
    # Heartbeat signal
    # ------------------------------------------------------------------

    def reset_timeout(self) -> None:
        """Signal that a valid leader message was received.

        Setting the event causes _timeout_loop to restart its countdown
        rather than firing an election.  Called from the /heartbeat and
        /vote endpoints in main.py.
        DDIA §5: follower resets its election timer on every heartbeat.
        """
        self._heartbeat_event.set()

    # ------------------------------------------------------------------
    # Vote handling
    # ------------------------------------------------------------------

    def handle_vote_request(
        self,
        candidate_id: str,
        candidate_term: int,
        candidate_lsn: int,
    ) -> tuple[bool, int, str | None]:
        """Evaluate an incoming vote request.

        Returns (vote_granted, current_term, reason).  reason is None
        on a grant, otherwise a short string explaining the denial.

        Grant conditions (DDIA §5.3 — all must hold):
          1. candidate_term >= current_term (not a stale leader)
          2. Node has not already voted for a different candidate this term
          3. candidate_lsn >= current_lsn (candidate log is at least as
             complete as ours — prevents electing a node that missed writes)

        Side-effects on grant:
          - If candidate_term > current_term, advances our term via
            set_term (caller handles any leader step-down this implies).
          - Records voted_for[term] = candidate_id.
          - Calls reset_timeout() so we don't start a competing election.

        Never raises.
        """
        current_term = self._get_term()
        current_lsn = self._get_lsn()

        # Reject a stale candidate — terms act as logical clocks.
        # DDIA §5: a node always rejects messages from a past term.
        if candidate_term < current_term:
            return False, current_term, "stale_term"

        # Advance term if the candidate is ahead; our voted_for record
        # for old terms is still valid so we don't clear it.
        if candidate_term > current_term:
            self._set_term(candidate_term)
            current_term = candidate_term

        # Only one vote per term; idempotent for the same candidate.
        existing_vote = self._voted_for.get(current_term)
        if existing_vote is not None and existing_vote != candidate_id:
            return False, current_term, "already_voted"

        # Candidate must have at least as many entries as us.
        # DDIA §5: only elect a leader with a complete enough log.
        if candidate_lsn < current_lsn:
            return False, current_term, "log_not_current"

        self._voted_for[current_term] = candidate_id
        # Granting a vote acknowledges this candidate as a potential
        # leader; reset the countdown to avoid a competing election.
        self.reset_timeout()
        logger.info(
            "Node %s granted vote to %s for term %d",
            self._node_id,
            candidate_id,
            current_term,
        )
        return True, current_term, None

    # ------------------------------------------------------------------
    # Background timeout loop
    # ------------------------------------------------------------------

    async def _timeout_loop(self) -> None:
        """Run the election countdown until the task is cancelled.

        Waits a randomised interval for a heartbeat event.  If the event
        does not arrive in time and this node is still a follower,
        triggers a candidate election.  If the node is already the leader,
        the timeout fires harmlessly and the loop restarts.
        DDIA §5: randomised timeouts reduce split-vote probability.
        """
        min_ms = self._cfg.get("election_timeout_min_ms", 600)
        max_ms = self._cfg.get("election_timeout_max_ms", 1000)

        while True:
            timeout_s = random.uniform(min_ms, max_ms) / 1000.0
            self._heartbeat_event.clear()

            try:
                await asyncio.wait_for(
                    self._heartbeat_event.wait(), timeout=timeout_s
                )
                # Heartbeat arrived before timeout — restart countdown.
            except asyncio.TimeoutError:
                if self._get_role() == "follower":
                    await self._run_election()

    # ------------------------------------------------------------------
    # Election execution
    # ------------------------------------------------------------------

    async def _run_election(self) -> None:
        """Promote self to candidate, solicit votes, become leader if won.

        Increments current_term, self-votes, and sends POST /vote to all
        peers concurrently.  Calls on_became_leader if a majority responds
        affirmatively within vote_timeout_ms.  Any peer that is
        unreachable or returns a higher term is treated as a non-vote.
        DDIA §5.2: election procedure.
        """
        new_term = self._get_term() + 1
        self._set_term(new_term)
        self._voted_for[new_term] = self._node_id  # self-vote
        my_lsn = self._get_lsn()

        logger.info(
            "Node %s starting election for term %d (lsn=%d)",
            self._node_id,
            new_term,
            my_lsn,
        )

        vote_timeout_s = (
            self._cfg.get("vote_timeout_ms", _DEFAULT_VOTE_TIMEOUT_MS)
            / 1000.0
        )

        async with httpx.AsyncClient() as client:
            results = await asyncio.gather(
                *[
                    self._request_vote(
                        client, peer, new_term, my_lsn, vote_timeout_s
                    )
                    for peer in self._peers
                ],
                return_exceptions=True,
            )

        votes = 1  # count our self-vote
        for result in results:
            if not isinstance(result, dict):
                continue
            if result.get("vote_granted"):
                votes += 1
            # If any peer reports a higher term, advance ours; we will
            # not have a majority and on_became_leader won't be called.
            peer_term = result.get("term", new_term)
            if peer_term > self._get_term():
                self._set_term(peer_term)

        majority = self._cfg.get("majority", 2)
        total = len(self._peers) + 1
        if votes >= majority:
            logger.info(
                "Node %s elected leader for term %d (%d/%d votes)",
                self._node_id,
                new_term,
                votes,
                total,
            )
            await self._on_became_leader(new_term)
        else:
            logger.info(
                "Node %s lost election for term %d (%d/%d votes)",
                self._node_id,
                new_term,
                votes,
                total,
            )

    async def _request_vote(
        self,
        client: httpx.AsyncClient,
        peer: dict,
        term: int,
        candidate_lsn: int,
        timeout_s: float,
    ) -> dict:
        """Send POST /vote to one peer and return the parsed response.

        Returns an empty dict on any network error or non-200 status so
        the caller can safely do result.get('vote_granted', False).
        Never raises.
        """
        url = f"http://{peer['host']}:{peer['port']}/vote"
        payload = {
            "candidate_id": self._node_id,
            "term": term,
            "candidate_lsn": candidate_lsn,
        }
        try:
            resp = await client.post(url, json=payload, timeout=timeout_s)
            if resp.status_code == 200:
                return resp.json()
            logger.debug(
                "Vote request to %s returned HTTP %d",
                peer["node_id"],
                resp.status_code,
            )
            return {}
        except Exception as exc:
            logger.debug(
                "Vote request to %s failed: %s", peer["node_id"], exc
            )
            return {}
