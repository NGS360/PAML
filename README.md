# Platform API Middle-Layer (PAML)

PAML(Platform API Middle-Layer) is an installable Python package to enable anyone to (easily) write cross-platform launchers.  It is used to abstract the specifics of a platform from workflow orchestration.  This abstraction of the platform layer enables the orchestration component to be platform independent to support interoperability.

## Contents

- [Features](#features)
- [Usage](#usage)
  - [Initial setup](#initial-setup)
- [FAQ](#faq)
- [Contributing](#contributing)

## Features

Multiple Supports Platforms

- Arvados
- SevenBridges

## Usage

### Initial setup

1. Create a virtual environment that your launcher will use

    ```{bash}
    python3 -m venv env
    source env/bin/activate
    ```

2. Install this package in your virtual environment

    ```{bash}
    pip install git+https://github.com/NGS360/PAML.git@<version>#egg=cwl_platform
    ```

    where `<version>` is the version you want to install

    Alternatively, if you've cloned this repo and want to install from source,

    ```{bash}
    pip install .
    ```

3. Develop Launcher

    Follow [ExampleLauncher](https://github.com/NGS360/ExampleLauncher) as an example

## FAQ

This repo hasn't been around long enough to earn a FAQ!

## Contributing

Contributions are always welcome!

If you find a bug :bug:, please open a [bug report](https://github.com/NGS360/PAML/issues/new/choose).

If you have an idea for an improvement or new feature :rocket:, please open a [feature request](https://github.com/NGS360/PAML/issues/new/choose).
