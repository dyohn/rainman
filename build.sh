#! /bin/bash

VERSION=3.13

# kill any previous virtualenv
rm -rf venv
python${VERSION} -m venv venv

# Update the package manager then install all dependencies
source venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade build
python3 -m pip install .

# Build the package
python3 -m build --wheel

# Shut down the virtualenv for now
deactivate