#!/usr/bin/env sh
# Regenerate the hashed lock files from requirements*.txt (uv). Run after
# editing either .txt file and commit the result; CI fails if they drift.
#
#   scripts/lock.sh                              # re-lock, keeping existing pins
#   scripts/lock.sh --upgrade-package httpx      # also move one package
#   scripts/lock.sh --upgrade                    # move everything (deliberately)
#
# --universal: one lock valid on every OS (markers, e.g. colorama on Windows);
# Python 3.12 is the Dockerfile base and the CI interpreter.
set -eu
cd "$(dirname "$0")/.."
for name in requirements requirements-dev; do
    uv pip compile "$name.txt" --universal --python-version 3.12 --generate-hashes \
        --custom-compile-command "scripts/lock.sh" --quiet -o "$name.lock" "$@"
done
