"""Cluster configuration loader.

Reads cluster.json and exposes helpers used by both the node
application and host-side scripts.  The config file path is resolved
from the CLUSTER_CONFIG environment variable; if unset it falls back
to the config/ directory at the repository root (correct for local dev
with an editable install).
"""

import json
import os

# Default: three levels up from this file (src/rainman/node/) → repo root
_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_MODULE_DIR, "../../.."))
_DEFAULT_CONFIG = os.path.join(_REPO_ROOT, "config", "cluster.json")


def load_cluster_config(path: str | None = None) -> dict:
    """Load and return the cluster configuration dict.

    Reads from *path* if given, otherwise from the CLUSTER_CONFIG
    environment variable, otherwise from the default repo-root location.
    Raises FileNotFoundError if the config file cannot be found, and
    json.JSONDecodeError if the file is malformed.
    """
    config_path = path or os.getenv("CLUSTER_CONFIG", _DEFAULT_CONFIG)
    with open(config_path) as f:
        return json.load(f)


def get_peers(config: dict, my_node_id: str) -> list[dict]:
    """Return all node descriptors except the one with my_node_id.

    Never raises; returns an empty list if my_node_id is not found.
    """
    return [n for n in config["nodes"] if n["node_id"] != my_node_id]


def get_static_leader(config: dict) -> dict:
    """Return the node descriptor with the lowest priority value.

    Used in Phase 2 where the highest-priority node (priority = 1)
    is always the leader.  Phase 3 replaces this with election.
    Raises ValueError if the nodes list is empty.
    """
    return min(config["nodes"], key=lambda n: n["priority"])
