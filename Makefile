all: test lint

install_dependencies:
	pip install --upgrade pip
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

build:
	python3 -m build

install: build
	python3 -m pip install --upgrade dist/*.whl

test:
	PYTHONPATH=src pytest --cov=src --cov-report=term --cov-report=html:htmlcov -v --log-cli-level=INFO

lint:
	pylint src tests scripts--max-line-length=120 --ignore-imports=y

