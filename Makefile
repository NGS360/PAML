virtual_env:
    python3 -m venv env
    source env/bin/activate
    python3 -m pip install --upgrade pip

build: virtual_env
    python3 -m pip install --upgrade build
    python3 -m build

install: virtual_env
    python3 -m pip install --upgrade dist/*.whl

test: virtual_env
    python3 -m pip install --upgrade pytest
    python3 -m pip install -r requirements.txt
    pytest
