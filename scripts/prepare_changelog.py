'''
This script creates a CHANGELOG.md based on the git history from the last version tag.
'''
import sys
from datetime import datetime
from pathlib import Path

import release_utils

REPO = sys.argv[1]
VERSION = sys.argv[2]


def get_commit_messages(version):
    '''
    Get commit messages between the last tag and HEAD.

    The endpoint is always HEAD: this runs before the release is tagged.
    '''
    commits = release_utils.commit_log(release_utils.previous_tag(version), "HEAD")

    lines = []
    for line in commits.strip().split("\n"):
        if not line.strip():
            continue
        # Strip the short hash prefix
        msg = line.split(" ", 1)[1] if " " in line else line
        lines.append(f"- {msg}\n")
    return lines


def main():
    ''' main function '''
    changelog = Path("CHANGELOG.md")

    with changelog.open(encoding='utf-8') as f:
        lines = f.readlines()

    insert_index = -1
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

    commit_lines = get_commit_messages(VERSION)

    new_lines = [
        "\n",
        f"## [v{VERSION}]({REPO}/releases/tag/v{VERSION}) - "
        f"{datetime.now().strftime('%Y-%m-%d')}\n",
        "\n",
    ]
    if commit_lines:
        new_lines.append("### Changed\n")
        new_lines.append("\n")
        new_lines.extend(commit_lines)
        new_lines.append("\n")

    lines[insert_index:insert_index] = new_lines

    with changelog.open("w", encoding="utf-8") as f:
        f.writelines(lines)

    print(f"CHANGELOG.md updated with v{VERSION} release notes.")


if __name__ == "__main__":
    main()
