#! /bin/bash

source venv/bin/activate
pytest tests --junitxml=unitTestsReport.xml
deactivate