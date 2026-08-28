# encoding: utf-8

"""
Prepares markdown release notes for GitHub releases.
"""

import os
import subprocess
from typing import List, Optional

import packaging.version

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


def git(*args: str) -> str:
    '''
    Run a git command and return its stdout.

    Unlike os.popen this captures stderr rather than letting it leak to the
    terminal, and raises on failure instead of quietly returning an empty
    string, so a broken command cannot pass for an empty result.
    '''
    result = subprocess.run(
        ["git", *args], capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed ({result.returncode}): "
            f"{result.stderr.strip()}"
        )
    return result.stdout


def rev_exists(ref: str) -> bool:
    ''' Whether a git revision resolves in this repository. '''
    return subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", ref],
        capture_output=True, check=False
    ).returncode == 0


def get_commit_history() -> str:
    '''
    get git commit history
    '''
    new_version = packaging.version.parse(TAG)

    # Get all tags sorted by version, latest first.
    all_tags = git("tag", "-l", "--sort=-version:refname", "v*").split("\n")

    # Out of `all_tags`, find the latest previous version so that we can collect all
    # commits between that version and the new version we're about to publish.
    # Note that we ignore pre-releases unless the new version is also a pre-release.
    last_tag: Optional[str] = None
    for tag in all_tags:
        if not tag.strip():  # could be blank line
            continue
        version = packaging.version.parse(tag)
        if new_version.pre is None and version.pre is not None:
            continue
        if version < new_version:
            last_tag = tag
            break
    # release.sh validates these notes before creating the tag, so TAG may not
    # resolve yet. HEAD is the commit that tag will point at, which makes the
    # preview accurate instead of making git complain about an unknown revision.
    end = TAG if rev_exists(TAG) else "HEAD"

    if last_tag is not None:
        commits = git("log", f"{last_tag}..{end}", "--oneline", "--first-parent")
    else:
        commits = git("log", "--oneline", "--first-parent")
    return "## Commits\n\n" + commits


def main():
    ''' main '''
    print(get_change_log_notes())
    print(get_commit_history())


if __name__ == "__main__":
    main()
