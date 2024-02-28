#!/bin/bash -e

TAG=$(grep version pyproject.toml | cut -d '=' -f2 | sed 's/"//g' | tr -d '[:space:]')

read -p "Creating new release for $TAG. Do you want to continue? [Y/n] " prompt

if [[ $prompt == "y" || $prompt == "Y" || $prompt == "yes" || $prompt == "Yes" ]]; then
    python scripts/prepare_changelog.py $TAG
    git add -A
    git commit -m "Bump version to $TAG for release" || true && git push
    echo "Creating new git tag $TAG"
    git tag "v$TAG" -m "v$TAG"
    git push --tags
else
    echo "Cancelled"
    exit 1
fi
