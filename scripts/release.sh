#!/bin/bash -e

# Configuration
REPO=https://github.com/NGS360/PAML
MAIN_BRANCH="main"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions
error() {
    echo -e "${RED}ERROR: $1${NC}" >&2
    exit 1
}

warning() {
    echo -e "${YELLOW}WARNING: $1${NC}"
}

success() {
    echo -e "${GREEN}$1${NC}"
}

# The package version is derived from the git tag by hatch-vcs, so there is
# nothing in pyproject.toml to read: the version is an input to this script and
# the tag it creates becomes the source of truth.
usage() {
    echo "Usage: $0 [VERSION]"
    echo "  VERSION  release version, e.g. 0.5.4 (a leading 'v' is accepted)"
    echo "           omit it and you will be prompted"
    exit 1
}

[[ "${1:-}" == "-h" || "${1:-}" == "--help" ]] && usage

TAG="${1:-}"
if [[ -z "$TAG" ]]; then
    read -p "Version to release (e.g. 0.5.4): " TAG
fi
TAG="${TAG#v}"
[[ -z "$TAG" ]] && error "No version given."

# Enforce MAJOR.MINOR.PATCH, with the pre-release suffixes DEV-006 allows.
# Historical tags like v0.5 and v0.3-rc1 predate this check; see issue #141.
if [[ ! "$TAG" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-(alpha|beta|rc)\.[0-9]+)?$ ]]; then
    error "Version '$TAG' is not MAJOR.MINOR.PATCH (optionally -alpha.N, -beta.N or -rc.N)."
fi

echo "=== Release Validation for v$TAG ==="
echo ""

# 1. Check if on main branch
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ "$CURRENT_BRANCH" != "$MAIN_BRANCH" ]]; then
    warning "Not on $MAIN_BRANCH branch (currently on: $CURRENT_BRANCH)"
    read -p "Continue anyway? [y/N] " prompt
    [[ ! $prompt =~ ^[Yy]$ ]] && error "Release cancelled. Switch to $MAIN_BRANCH branch first."
fi

# 2. Check for uncommitted changes
if [[ -n $(git status --porcelain) ]]; then
    warning "Working directory has uncommitted changes:"
    git status --short
    read -p "Continue anyway? [y/N] " prompt
    [[ ! $prompt =~ ^[Yy]$ ]] && error "Release cancelled. Commit or stash changes first."
fi

# 3. Check if tag already exists
if git rev-parse "v$TAG" >/dev/null 2>&1; then
    error "Tag v$TAG already exists. Choose a different version."
fi

# 4. Check if remote tag exists
if git ls-remote --tags origin | grep -q "refs/tags/v$TAG"; then
    error "Tag v$TAG already exists on remote. Choose a different version."
fi

# 5. Check if tests exist and offer to run them
if compgen -G "tests/test_*.py" > /dev/null || compgen -G "test_*.py" > /dev/null || [[ -f "pytest.ini" ]]; then
    read -p "Run tests before release? [Y/n] " prompt
    if [[ $prompt == "n" || $prompt == "N" || $prompt == "no" || $prompt == "No" ]]; then
        warning "Skipping tests"
    else
        echo "Running tests..."
        # Run through uv so the versions match uv.lock and CI, rather than
        # whatever happens to be on PATH.
        if ! uv run pytest; then
            error "Tests failed. Fix tests before releasing."
        fi
        success "Tests passed ✓"
    fi
else
    warning "No tests found to run"
fi

# 6. Fetch latest from remote
echo "Fetching latest changes from remote..."
git fetch origin

# 7. Check if local is behind remote
LOCAL=$(git rev-parse @)
REMOTE=$(git rev-parse @{u} 2>/dev/null || echo "")
if [[ -n "$REMOTE" ]] && [[ "$LOCAL" != "$REMOTE" ]]; then
    BASE=$(git merge-base @ @{u})
    if [[ "$LOCAL" = "$BASE" ]]; then
        error "Local branch is behind remote. Run 'git pull' first."
    elif [[ "$REMOTE" != "$BASE" ]]; then
        warning "Local and remote have diverged"
    fi
fi

# 8. Verify CI is green for the commit being released
SHA=$(git rev-parse HEAD)
SHORT_SHA=$(git rev-parse --short HEAD)
if ! command -v gh > /dev/null 2>&1 || ! gh auth status > /dev/null 2>&1; then
    warning "GitHub CLI unavailable or not authenticated - cannot check CI status"
    read -p "Continue without verifying CI? [y/N] " prompt
    [[ ! $prompt =~ ^[Yy]$ ]] && error "Release cancelled. Install/authenticate 'gh', or verify CI manually."
else
    echo "Checking CI status for $SHORT_SHA..."
    RUN_COUNT=$(gh run list --commit "$SHA" --limit 50 --json status --jq 'length' 2>/dev/null || echo "0")
    FAILED=$(gh run list --commit "$SHA" --limit 50 --json conclusion,workflowName \
        --jq '[.[] | select(.conclusion == "failure" or .conclusion == "cancelled" or .conclusion == "timed_out") | .workflowName] | unique | join(", ")' 2>/dev/null || echo "")
    PENDING=$(gh run list --commit "$SHA" --limit 50 --json status,workflowName \
        --jq '[.[] | select(.status != "completed") | .workflowName] | unique | join(", ")' 2>/dev/null || echo "")

    if [[ "$RUN_COUNT" == "0" ]]; then
        warning "No CI runs found for $SHORT_SHA. Has this commit been pushed?"
        read -p "Continue without a CI result? [y/N] " prompt
        [[ ! $prompt =~ ^[Yy]$ ]] && error "Release cancelled. Push the commit and wait for CI."
    elif [[ -n "$FAILED" ]]; then
        warning "CI is FAILING for $SHORT_SHA: $FAILED"
        read -p "Release anyway? [y/N] " prompt
        [[ ! $prompt =~ ^[Yy]$ ]] && error "Release cancelled. Fix CI before releasing."
    elif [[ -n "$PENDING" ]]; then
        warning "CI is still running for $SHORT_SHA: $PENDING"
        read -p "Release before CI completes? [y/N] " prompt
        [[ ! $prompt =~ ^[Yy]$ ]] && error "Release cancelled. Wait for CI to finish."
    else
        success "CI is green ✓"
    fi
fi

# 9. Check CHANGELOG.md exists
if [[ ! -f "CHANGELOG.md" ]]; then
    warning "CHANGELOG.md not found"
    read -p "Create a basic CHANGELOG.md? [Y/n] " prompt
    if [[ ! $prompt =~ ^[Nn]$ ]]; then
        cat > CHANGELOG.md << EOF
# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

EOF
        git add CHANGELOG.md
        success "Created CHANGELOG.md"
    fi
fi

# Which half of the release is this? The CHANGELOG reaches main through a pull
# request, because main does not accept direct pushes, so a release takes two
# runs of this script: the first opens that PR, the second tags the merge commit
# once it has landed.
if git show "origin/$MAIN_BRANCH:CHANGELOG.md" 2>/dev/null | grep -q "^## \[v$TAG\]"; then
    PHASE="tag"
else
    PHASE="changelog"
fi

echo ""
echo "=== Release Summary ==="
echo "Version: v$TAG"
echo "Branch: $CURRENT_BRANCH"
echo "Repository: $REPO"
if [[ "$PHASE" == "changelog" ]]; then
    echo "Step: 1 of 2 - open the CHANGELOG pull request"
else
    echo "Step: 2 of 2 - tag the merged release commit"
fi
echo ""

if [[ "$PHASE" == "changelog" ]]; then
    read -p "Prepare the CHANGELOG for v$TAG and open a pull request? [Y/n] " prompt
    if [[ $prompt == "n" || $prompt == "N" || $prompt == "no" || $prompt == "No" ]]; then
        echo "Cancelled"
        exit 1
    fi

    RELEASE_BRANCH="release/v$TAG"
    if git rev-parse --verify "$RELEASE_BRANCH" >/dev/null 2>&1; then
        error "Branch $RELEASE_BRANCH already exists locally. Delete it or finish that release."
    fi
    if git ls-remote --heads origin "$RELEASE_BRANCH" | grep -q "$RELEASE_BRANCH"; then
        error "Branch $RELEASE_BRANCH already exists on the remote. Finish or delete that release."
    fi

    echo ""
    echo "Creating $RELEASE_BRANCH..."
    git checkout -q -b "$RELEASE_BRANCH"

    echo "Preparing CHANGELOG..."
    python3 scripts/prepare_changelog.py "$REPO" "$TAG"

    echo ""
    warning "CHANGELOG has been updated. Please review and edit if needed."
    echo "Press Enter to continue after reviewing CHANGELOG.md, or Ctrl+C to cancel..."
    read

    # Validate that release notes can be generated before anything is pushed. The
    # same generation step runs in GitHub Actions once the tag exists, where
    # recovery would mean deleting an already-published tag.
    echo "Validating release notes for v$TAG..."
    if ! TAG="v$TAG" python3 scripts/release_notes.py > /dev/null; then
        error "Could not generate release notes for v$TAG. Fix CHANGELOG.md and re-run."
    fi
    success "Release notes validated ✓"

    echo "Committing CHANGELOG..."
    git add CHANGELOG.md
    if ! git commit -q -m "Update CHANGELOG for v$TAG"; then
        error "Nothing to commit. Does CHANGELOG.md already contain a v$TAG section?"
    fi
    success "Changes committed ✓"

    echo "Pushing $RELEASE_BRANCH..."
    git push -q -u origin "$RELEASE_BRANCH"
    success "Branch pushed ✓"

    # Checked explicitly rather than left to 'set -e': the branch is already
    # pushed at this point, so a failure here needs a message that says so.
    echo "Opening pull request..."
    if ! gh pr create --base "$MAIN_BRANCH" --head "$RELEASE_BRANCH" \
        --title "Release v$TAG" \
        --body "CHANGELOG entry for v$TAG. Merging this creates the commit that will be tagged; re-run 'scripts/release.sh $TAG' afterwards to tag it."; then
        error "Could not open the pull request. $RELEASE_BRANCH is pushed, so open it manually and then re-run this script."
    fi

    echo ""
    success "=== Step 1 of 2 complete ==="
    echo "Review and merge the pull request above, then run:"
    echo "  git checkout $MAIN_BRANCH && git pull"
    echo "  ./scripts/release.sh $TAG"
    exit 0
fi

# PHASE=tag. The CHANGELOG for this version is already on main, so the only
# remaining work is tagging the commit that carries it. Unlike the old flow, that
# commit has been through CI as part of its pull request.
if ! grep -q "^## \[v$TAG\]" CHANGELOG.md; then
    error "CHANGELOG.md here has no v$TAG section even though $MAIN_BRANCH does. Run 'git pull'."
fi

echo "Validating release notes for v$TAG..."
if ! TAG="v$TAG" python3 scripts/release_notes.py > /dev/null; then
    error "Could not generate release notes for v$TAG. Fix CHANGELOG.md on $MAIN_BRANCH first."
fi
success "Release notes validated ✓"

read -p "Tag v$TAG at $(git rev-parse --short HEAD) and push? [Y/n] " prompt
if [[ $prompt == "n" || $prompt == "N" || $prompt == "no" || $prompt == "No" ]]; then
    echo "Cancelled"
    exit 1
fi

echo "Creating git tag v$TAG..."
git tag "v$TAG" -m "v$TAG"
success "Tag created ✓"

echo "Pushing tag to remote..."
git push origin "v$TAG"
success "Tag pushed ✓"

echo ""
success "=== Release v$TAG initiated successfully! ==="
echo "GitHub Actions will now build and create a draft release."
echo "Visit: $REPO/releases to review and publish the release."
