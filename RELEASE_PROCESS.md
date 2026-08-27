# Release Process

The scripts in `scripts/` were originally based on [Python Package
Template](https://github.com/allenai/python-package-template) and have since
diverged from it.

## Prerequisites

Before starting a release, ensure:

- All tests pass: `make test`
- Working directory is clean: `git status`
- On the main branch with latest changes: `git pull origin main`
- The changes you are releasing are already pushed **and CI has finished on
  them**. `release.sh` reads the result, and stops if there is nothing to read
- GitHub CLI installed and authenticated: `gh auth status`. This is what the CI
  check uses; without it the script can only ask you to confirm blind
- CHANGELOG.md exists (will be auto-created if missing)

## Quick Release

1. **Bump version** in `pyproject.toml`

   Update the version number (e.g., `0.5.3` → `0.5.4`):
   ```toml
   [project]
   name = "cwl_platform"
   version = "0.5.4"  # Update this line
   ```

2. **Run the release script:**
   ```bash
   ./scripts/release.sh
   ```
   
   The script will:
   - Validate your environment (branch, uncommitted changes, etc.)
   - Optionally run tests
   - Check that CI is green for the commit you are releasing from
   - Generate CHANGELOG entries from git commits
   - Pause for you to review/edit CHANGELOG.md
   - Verify release notes can be generated (before any tag is created)
   - Commit version and CHANGELOG changes
   - Create and push a git tag (e.g., `v0.5.4`)
   - Trigger GitHub Actions to build the package and open a draft release

3. **Finalize on GitHub:**
   - Navigate to https://github.com/NGS360/PAML/releases
   - Review the draft release created by GitHub Actions
   - Verify release notes and artifacts
   - Click **"Publish release"**

## Troubleshooting

### Failed Release Recovery

If the GitHub Actions workflow fails after the tag is pushed, re-run the build
against the existing tag rather than deleting it. Which path you take depends on
where the fault was.

**If the workflow or its tooling was at fault** — a missing permission, a broken
step, a dependency that would not install — the tagged code is fine. A manual run
reads the workflow definition from `main` but builds the code from the tag, so
fixing `main` is enough:

1. Fix the workflow and merge it to `main`
2. Delete the draft release if one was created, since `gh release create` will
   not overwrite an existing release:
   ```bash
   gh release delete v0.5.4 --yes
   ```
3. Re-run the build for that tag, from the Actions tab or the CLI:
   ```bash
   gh workflow run release.yml -f tag=v0.5.4
   ```

**If the tagged content was at fault** — a wrong version number, a malformed
CHANGELOG, a bug that should not ship — then the tag points at something you do
not want to release. Do not move the tag; release a new patch version instead,
starting from the top of this document.

Avoid deleting a tag once it has been pushed. Anyone who already installed that
version resolved it to a specific commit, and recreating the tag hands them
different code under the same name. `release.sh` now validates the CHANGELOG
before it creates the tag, so the most common cause of this failure is caught
while recovery is still free.

### Editing CHANGELOG Before Release

The release script pauses after generating CHANGELOG entries. At this point:

1. Review the auto-generated changes in `CHANGELOG.md`
2. Edit to add context, group related changes, or improve descriptions
3. Press Enter to continue with the release

### Rollback a Published Release

This package is not uploaded to a package index. Consumers install straight from
a git tag (see the README), so a release stays reachable for as long as its tag
exists and there is nothing to yank.

To retract a published release:

1. **Mark the release as a pre-release** on GitHub so it no longer shows as the
   latest, and describe the problem in its release notes
2. **Leave the tag in place.** Anyone who pinned that version already resolved
   it, and deleting or moving a published tag breaks their installs
3. **Release a new patch version** with the fix (e.g., `0.5.5`) and direct
   people to it

## Release Script Validations

The `release.sh` script performs these checks:

- ✓ Extracts version from `pyproject.toml`
- ✓ Verifies you're on the `main` branch
- ✓ Checks for uncommitted changes
- ✓ Confirms tag doesn't already exist (local and remote)
- ✓ Offers to run tests before releasing (via `PYTHONPATH=src pytest`, matching CI)
- ✓ Ensures local branch is up-to-date with remote
- ✓ Checks GitHub Actions CI status for `HEAD` before tagging (requires `gh`)
- ✓ Creates `CHANGELOG.md` if missing
- ✓ Validates that release notes can be generated **before** creating the tag

The release-notes validation matters because the same generation step runs in
GitHub Actions *after* the tag is pushed. Catching a malformed CHANGELOG locally
avoids having to delete an already-published tag.

Note what the CI check does and does not cover. It reads the result for the
commit you are releasing *from*. The version-bump commit that the tag actually
lands on is created afterwards, so its own CI run is still in flight when the tag
is pushed — it changes only `pyproject.toml` and `CHANGELOG.md`, but the gate is
not a guarantee about the tagged commit itself.

## Version Numbering

Follow [Semantic Versioning](https://semver.org/):

- **Patch** (0.5.1 → 0.5.2): Bug fixes, minor changes
- **Minor** (0.5.2 → 0.6.0): New features, backwards-compatible
- **Major** (0.6.0 → 1.0.0): Breaking changes

## What Happens on GitHub Actions

When you push a tag, the `.github/workflows/release.yml` workflow:

1. Checks out the code
2. Builds the Python package (`python3 -m build`)
3. Validates the built artifacts (`twine check`)
4. Generates release notes from `CHANGELOG.md`
5. Creates a **draft release** on GitHub with the built artifacts attached
6. Waits for you to manually publish the release

The workflow can also be started by hand from the Actions tab. Give it an
existing tag to rebuild that tag's draft release, or leave the tag empty to
confirm the package still builds without creating a release.
