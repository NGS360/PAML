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
	PYTHONPATH=src coverage run --source=src -m pytest -v
	coverage report
	coverage html

lint:
	find . -name "*.py" | xargs pylint --max-line-length=120 --ignore-imports=y

