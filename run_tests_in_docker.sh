#!/bin/bash -e
# Run using:
# docker run --rm -ti -v $PWD:/cwl_platform -w /cwl_platform python:3.12 ./run_tests_in_docker.sh

# uv creates and manages the virtual environment itself, so no venv setup here.
pip install --quiet uv

make install_dependencies
make test
make lint
