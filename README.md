# Platform API Middle-Layer (PAML)

PAML(Platform API Middle-Layer) is an installable Python package to enable anyone to (easily) write cross-platform launchers.  It is used to abstract the specifics of a platform from workflow orchestration.  This abstraction of the platform layer enables the orchestration component to be platform independent to support interoperability.

## Contents

- [Features](#features)
- [Usage](#usage)
  - [Initial setup](#initial-setup)
- [Projects using this template](#projects-using-this-template)
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
    pip install git+https://biogit.pri.bms.com/NGS/cwl_platform.git@<version>#egg=cwl_platform
    ```

    where `<version>` is the version you want to install

    Alternatively, if you've cloned this repo and want to install from source,

    ```{bash}
    pip install .
    ```

3. Develop Launcher

    Follow [ExampleCICDPipelineDeployment](https://biogit.pri.bms.com/NGS/ExampleCICDPipelineDeployment), or any of the other launchers listed below, as an example
    
    If you have an existing launcher, perform the following steps:

    ```
    git rm launcher/src/cwl_platform
    ```
    
    - Install this package to your requirements.txt that is used by your launcher's Dockerfile.

## Projects using this template

These are the launchers I know of

- [ExampleCICDPipelineDeployment](https://biogit.pri.bms.com/NGS/ExampleCICDPipelineDeployment)
- [bulk RNA-Seq](https://github.com/bmsgh/RNA-Seq-Launcher)
- [Whole Exome](https://github.com/bmsgh/WES-Launcher-New)
- [CNV](https://github.com/bmsgh/CNV-and-LOH)

## FAQ

This repo hasn't been around long enough to earn a FAQ!

## Contributing

Contributions are always welcome!

If you find a bug :bug:, please open a [bug report](https://biogit.pri.bms.com/NGS/cwl_platform/issues/new/choose).
If you have an idea for an improvement or new feature :rocket:, please open a [feature request](https://biogit.pri.bms.com/NGS/cwl_platform/issues/new/choose).
