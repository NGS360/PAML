#!/bin/bash -e
# Run using:
# docker run --rm -ti -v $PWD:/cwl_platform -w /cwl_platform python:3.9 ./run_tests_in_docker.sh

python3 -m venv ~/env
source ~/env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install mock
make test
make lint