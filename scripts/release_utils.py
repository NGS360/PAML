# encoding: utf-8

"""
Git helpers shared by the release scripts.

release_notes.py and prepare_changelog.py both need to work out which commits
belong to a release, and previously each carried its own copy of that logic.
"""

import subprocess
from typing import Optional

import packaging.version


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


def previous_tag(version: str) -> Optional[str]:
    '''
    The highest v* tag older than the given version, or None if there is none.

    Pre-release tags are skipped unless the version being released is itself a
    pre-release, so a final release is described relative to the last final
    release rather than to an intervening release candidate.

    The version may be given with or without its leading 'v'.
    '''
    new_version = packaging.version.parse(version)
    for tag in git("tag", "-l", "--sort=-version:refname", "v*").split("\n"):
        if not tag.strip():
            continue
        candidate = packaging.version.parse(tag)
        if new_version.pre is None and candidate.pre is not None:
            continue
        if candidate < new_version:
            return tag
    return None


def commit_log(start_tag: Optional[str], end: str) -> str:
    '''
    First-parent one-line log for the commits a release contains.

    Passing start_tag=None means there is no earlier tag, so the whole history
    up to `end` is returned.
    '''
    if start_tag is not None:
        return git("log", f"{start_tag}..{end}", "--oneline", "--first-parent")
    return git("log", "--oneline", "--first-parent")
