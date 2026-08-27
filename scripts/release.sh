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

# Extract version from pyproject.toml
TAG=$(grep version pyproject.toml | cut -d '=' -f2 | sed 's/"//g' | tr -d '[:space:]')
[[ -z "$TAG" ]] && error "Could not extract version from pyproject.toml"

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
    error "Tag v$TAG already exists. Update version in pyproject.toml first."
fi

# 4. Check if remote tag exists
if git ls-remote --tags origin | grep -q "refs/tags/v$TAG"; then
    error "Tag v$TAG already exists on remote. Update version in pyproject.toml first."
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

echo ""
echo "=== Release Summary ==="
echo "Version: v$TAG"
echo "Branch: $CURRENT_BRANCH"
echo "Repository: $REPO"
echo ""

read -p "Creating new release for v$TAG. Do you want to continue? [Y/n] " prompt

if [[ $prompt == "y" || $prompt == "Y" || $prompt == "yes" || $prompt == "Yes" || $prompt == "" ]]; then
    echo ""
    echo "Preparing CHANGELOG..."
    python3 scripts/prepare_changelog.py $REPO $TAG

    echo ""
    warning "CHANGELOG has been updated. Please review and edit if needed."
    echo "Press Enter to continue after reviewing CHANGELOG.md, or Ctrl+C to cancel..."
    read

    # Validate that release notes can be generated BEFORE creating the tag.
    # The GitHub Actions workflow runs this same script on the pushed tag; if it
    # fails there, recovery requires deleting an already-published tag.
    echo "Validating release notes for v$TAG..."
    if ! TAG="v$TAG" python3 scripts/release_notes.py > /dev/null; then
        error "Could not generate release notes for v$TAG. Fix CHANGELOG.md and re-run (no tag was created)."
    fi
    success "Release notes validated ✓"

    echo ""
    echo "Committing changes..."
    git add pyproject.toml CHANGELOG.md
    if git commit -m "Bump version to $TAG for release"; then
        success "Changes committed ✓"
    else
        warning "No changes to commit (this is OK if version was already committed)"
    fi

    echo "Pushing to remote..."
    git push
    success "Changes pushed ✓"

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
else
    echo "Cancelled"
    exit 1
fi
