# encoding: utf-8

"""
Prepares markdown release notes for GitHub releases.
"""

import os
from typing import List

import release_utils

TAG = os.environ["TAG"]

ADDED_HEADER = "### Added 🎉"
CHANGED_HEADER = "### Changed ⚠️"
FIXED_HEADER = "### Fixed ✅"
REMOVED_HEADER = "### Removed 👋"


def get_change_log_notes() -> str:
    '''
    Read CHANGELOG.md
    '''
    in_current_section = False
    current_section_notes: List[str] = []
    with open("CHANGELOG.md", encoding="utf-8") as changelog:
        for line in changelog:
            if line.startswith("## "):
                if line.startswith("## Unreleased"):
                    continue
                if line.startswith(f"## [{TAG}]"):
                    in_current_section = True
                    continue
                break
            if in_current_section:
                if line.startswith("### Added"):
                    line = ADDED_HEADER + "\n"
                elif line.startswith("### Changed"):
                    line = CHANGED_HEADER + "\n"
                elif line.startswith("### Fixed"):
                    line = FIXED_HEADER + "\n"
                elif line.startswith("### Removed"):
                    line = REMOVED_HEADER + "\n"
                current_section_notes.append(line)
    if not current_section_notes:
        raise SystemExit(
            f"ERROR: No release notes found for {TAG} in CHANGELOG.md.\n"
            f"Expected a non-empty '## [{TAG}](...)' section listing the changes "
            f"for this release."
        )
    return "## What's new\n\n" + "".join(current_section_notes).strip() + "\n"


def get_commit_history() -> str:
    '''
    get git commit history
    '''
    # release.sh validates these notes before creating the tag, so TAG may not
    # resolve yet. HEAD is the commit that tag will point at, which makes the
    # preview accurate instead of making git complain about an unknown revision.
    end = TAG if release_utils.rev_exists(TAG) else "HEAD"
    commits = release_utils.commit_log(release_utils.previous_tag(TAG), end)
    return "## Commits\n\n" + commits


def main():
    ''' main '''
    print(get_change_log_notes())
    print(get_commit_history())


if __name__ == "__main__":
    main()
