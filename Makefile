all: test lint

build:
	python3 -m pip install --upgrade build
	python3 -m build

install: build
	python3 -m pip install --upgrade dist/*.whl

test:
	python3 -m pip install --upgrade pytest mock coverage
	python3 -m pip install -r requirements.txt
	PYTHONPATH=src coverage run --source=src -m pytest
	coverage report
	coverage html

lint:
	python3 -m pip install --upgrade pylint
	pylint --max-line-length=120 --ignore-imports=y --exit-zero $$(git ls-files '*.py')
