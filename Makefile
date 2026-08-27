all: test lint

install_dependencies:
	uv sync

build:
	uv build

install: build
	uv pip install --upgrade dist/*.whl

test:
	uv run coverage run --source=src -m pytest -v --log-cli-level=INFO
	uv run coverage report
	uv run coverage html -d htmlcov

lint:
	uv run pylint src tests scripts
