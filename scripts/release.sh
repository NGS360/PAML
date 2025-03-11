#!/bin/bash -e

REPO=https://github.com/NGS360/PAML.git
TAG=$(grep version pyproject.toml | cut -d '=' -f2 | sed 's/"//g' | tr -d '[:space:]')

read -p "Creating new release for v$TAG. Do you want to continue? [Y/n] " prompt

if [[ $prompt == "y" || $prompt == "Y" || $prompt == "yes" || $prompt == "Yes" ]]; then
    python3 scripts/prepare_changelog.py $REPO $TAG
    # It appears here is where you need to manually add information to the CHANGELOG.md
    git add -A
    git commit -m "Bump version to $TAG for release" || true && git push
    echo "Creating new git tag $TAG"
    git tag "v$TAG" -m "v$TAG"
    git push --tags
else
    echo "Cancelled"
    exit 1
fi
