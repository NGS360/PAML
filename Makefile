all: test

virtual_env:
	python3 -m venv env
	source env/bin/activate
	python3 -m pip install --upgrade pip

build: 
	python3 -m pip install --upgrade build
	python3 -m build

install: build
	python3 -m pip install --upgrade dist/*.whl

test: install
	python3 -m pip install --upgrade pytest mock
	python3 -m pip install -r requirements.txt
	pytest

lint:
	python3 -m pip install --upgrade pylint
	pylint --max-line-length=120 --ignore-imports=y $$(git ls-files '*.py')
