'''
This script creates a CHANGELOG.md based on the git history from the last version tag.
'''
from datetime import datetime
from pathlib import Path
import sys

#from my_package.version import VERSION
REPO=sys.argv[1]
VERSION=sys.argv[2]

def main():
    ''' main function '''
    changelog = Path("CHANGELOG.md")

    with changelog.open(encoding='ascii') as f:
        lines = f.readlines()

    insert_index: int = -1
    for i, line in enumerate(lines):
        if line.startswith("## Unreleased"):
            insert_index = i + 1
        elif line.startswith(f"## [v{VERSION}]"):
            print("CHANGELOG already up-to-date")
            return
        elif line.startswith("## [v"):
            break

    if insert_index < 0:
        raise RuntimeError("Couldn't find 'Unreleased' section")

    lines.insert(insert_index, "\n")
    lines.insert(
        insert_index + 1,
        f"## [v{VERSION}]({REPO}/releases/tag/v{VERSION}) - "
        f"{datetime.now().strftime('%Y-%m-%d')}\n",
    )

    with changelog.open("w", encoding="ascii") as f:
        f.writelines(lines)


if __name__ == "__main__":
    main()
