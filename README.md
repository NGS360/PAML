# Platform API Middle-Layer (PAML)

PAML(Platform API Middle-Layer) is an installable Python package to enable anyone to (easily) write cross-platform launchers.  It is used to abstract the specifics of a platform from workflow orchestration.  This abstraction of the platform layer enables the orchestration component to be platform independent to support interoperability.

## Contents

- [Features](#features)
- [Usage](#usage)
  - [Initial setup](#initial-setup)
- [Versioning](#versioning)
- [FAQ](#faq)
- [Contributing](#contributing)

## Features

Multiple supported platforms

- Arvados
- SevenBridges
- NGS360 (via the GA4GH WES API)

## Usage

### Initial setup

1. Create a virtual environment that your launcher will use

    ```{bash}
    python3 -m venv env
    source env/bin/activate
    ```

2. Install this package in your virtual environment

    ```{bash}
    pip install git+https://github.com/NGS360/PAML.git@<tag>#egg=cwl_platform
    ```

    where `<tag>` is a release tag such as `v0.5.3`, listed on the
    [releases page](https://github.com/NGS360/PAML/releases). The `v` is part of
    the tag name and is required - `@0.5.3` does not resolve to anything.

    Alternatively, if you've cloned this repo and want to install from source,

    ```{bash}
    pip install .
    ```

3. Develop Launcher

    Follow [ExampleLauncher](https://github.com/NGS360/ExampleLauncher) as an example

## Versioning

PAML follows [Semantic Versioning](https://semver.org/). A release is a git tag
of the form `vMAJOR.MINOR.PATCH`, and the package version is derived from that
tag, so an installed build always reports where it came from:

```{python}
import cwl_platform
print(cwl_platform.__version__)
```

- **PATCH** (`v0.5.1` to `v0.5.2`) - bug fixes and backwards-compatible changes
- **MINOR** (`v0.5.2` to `v0.6.0`) - new backwards-compatible functionality
- **MAJOR** (`v0.6.0` to `v1.0.0`) - breaking changes

**The version is below `1.0.0`, so the API is not yet stable.** SemVer permits a
`0.x` minor release to break compatibility, so pin an exact tag rather than
tracking a branch, and check the [CHANGELOG](CHANGELOG.md) before upgrading.

A few tags predating this convention have only two components (`v0.5`, for
example). They are left as they are, because moving a published tag would break
anyone already pinned to it.

See [RELEASE_PROCESS.md](RELEASE_PROCESS.md) for how releases are made.

## FAQ

This repo hasn't been around long enough to earn a FAQ!

## Contributing

Contributions are always welcome!

If you find a bug :bug:, please open a [bug report](https://github.com/NGS360/PAML/issues/new/choose).

If you have an idea for an improvement or new feature :rocket:, please open a [feature request](https://github.com/NGS360/PAML/issues/new/choose).

## Acknowledgements

* Maggie Chen
* Steve Vasquez-Grinnell
