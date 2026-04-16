#!/usr/bin/env sh
set -e

if ! command -v grype > /dev/null 2>&1; then
    echo "grype not installed: brew install grype"
    exit 1
fi

grype dir:. --fail-on high --quiet
