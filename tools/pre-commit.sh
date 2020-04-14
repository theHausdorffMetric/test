#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

# Find the root commit
# Note: the --fork-point would gave a better root,
# but this option requires git 2.15+ which is not yet available on our CI
# cf: https://git-scm.com/docs/git-merge-baselse
ROOT=$(git merge-base origin/master HEAD)

CHANGED="$(git diff --diff-filter=ACMR --name-only ${ROOT}..HEAD)"
echo "Modified files:"
echo $CHANGED
echo

pre-commit run --files $CHANGED
