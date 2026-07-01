'''
This script creates a CHANGELOG.md based on the git history from the last version tag.
'''
import os
import sys
from datetime import datetime
from pathlib import Path

import packaging.version

REPO = sys.argv[1]
VERSION = sys.argv[2]


def get_commit_messages(version):
    '''Get commit messages between the last tag and HEAD.'''
    new_version = packaging.version.parse(version)

    all_tags = os.popen("git tag -l --sort=-version:refname 'v*'").read().split("\n")

    last_tag = None
    for tag in all_tags:
        if not tag.strip():
            continue
        v = packaging.version.parse(tag)
        if new_version.pre is None and v.pre is not None:
            continue
        if v < new_version:
            last_tag = tag
            break

    if last_tag is not None:
        commits = os.popen(f"git log {last_tag}..HEAD --oneline --first-parent").read()
    else:
        commits = os.popen("git log --oneline --first-parent").read()

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
