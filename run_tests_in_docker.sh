#!/bin/bash -e
# Run using:
# docker run --rm -ti -v $PWD:/cwl_platform -w /cwl_platform python:3.9 ./run_tests_in_docker.sh

python3 -m venv ~/env
source ~/env/bin/activate

make install_dependencies
make test
make lint