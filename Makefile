all: test lint

install_dependencies:
	pip install --upgrade pip
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

build:
	python3 -m build

install: build
	python3 -m pip install --upgrade dist/*.whl

test: # This may not work on Mac due to pycurl issues.  Use run_tests_in_docker.sh instead
	PYTHONPATH=src coverage run --source=src -m pytest
	coverage report
	coverage html

lint:
	find . -name "*.py" | xargs pylint --max-line-length=120 --ignore-imports=y --exit-zero

