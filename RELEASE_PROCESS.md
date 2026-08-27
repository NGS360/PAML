# Release Process

This follows the guidance of [Python Package Template](https://github.com/allenai/python-package-template)

## Prerequisites

Before starting a release, ensure:

- All tests pass: `pytest`
- Working directory is clean: `git status`
- On the main branch with latest changes: `git pull origin main`
- CHANGELOG.md exists (will be auto-created if missing)

## Quick Release

1. **Bump version** in `pyproject.toml`

   Update the version number (e.g., `0.5.1` → `0.5.2`):
   ```toml
   [project]
   name = "cwl_platform"
   version = "0.5.2"  # Update this line
   ```

2. **Run the release script:**
   ```bash
   ./scripts/release.sh
   ```
   
   The script will:
   - Validate your environment (branch, uncommitted changes, etc.)
   - Optionally run tests
   - Check that CI is green for the commit being released
   - Generate CHANGELOG entries from git commits
   - Pause for you to review/edit CHANGELOG.md
   - Verify release notes can be generated (before any tag is created)
   - Commit version and CHANGELOG changes
   - Create and push a git tag (e.g., `v0.5.2`)
   - Trigger GitHub Actions to build the package and open a draft release

3. **Finalize on GitHub:**
   - Navigate to https://github.com/NGS360/PAML/releases
   - Review the draft release created by GitHub Actions
   - Verify release notes and artifacts
   - Click **"Publish release"**

## Troubleshooting

### Failed Release Recovery

If the GitHub Actions workflow fails after the tag is pushed:

1. **Delete the failed tag:**
   ```bash
   TAG="v0.5.2"  # Replace with your version
   git tag -d $TAG              # Delete locally
   git push --delete origin $TAG  # Delete from GitHub
   ```

2. **Delete the draft release on GitHub** (if one was created):
   - Go to https://github.com/NGS360/PAML/releases
   - Find the draft release and delete it

3. **Fix the issue**, then repeat the release steps above

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
3. **Release a new patch version** with the fix (e.g., `0.5.3`) and direct
   people to it

## Release Script Validations

The `release.sh` script performs these checks:

- ✓ Extracts version from `pyproject.toml`
- ✓ Verifies you're on the `main` branch
- ✓ Checks for uncommitted changes
- ✓ Confirms tag doesn't already exist (local and remote)
- ✓ Offers to run tests before releasing (via `PYTHONPATH=src pytest`, matching CI)
- ✓ Ensures local branch is up-to-date with remote
- ✓ Checks GitHub Actions CI status for the commit being released (requires `gh`)
- ✓ Creates `CHANGELOG.md` if missing
- ✓ Validates that release notes can be generated **before** creating the tag

The release-notes validation matters because the same generation step runs in
GitHub Actions *after* the tag is pushed. Catching a malformed CHANGELOG locally
avoids having to delete an already-published tag.

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
