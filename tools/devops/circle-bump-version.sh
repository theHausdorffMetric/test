#!/usr/bin/env bash

# Copied and modified from - https://discuss.circleci.com/t/circleci-pypi-deploy/11818/2

if [ -z "$CI" ]; then
    echo "will only continue on CI"
    exit
fi

if [[ $CIRCLE_BRANCH != "master" ]]; then
    echo "will only continue for master builds"
    exit
fi

if [ -z "$KPLER_GITHUB_PUSH_TOKEN" ]; then
    echo "no github token found in env, cannot continue"
    exit
fi

# makegithub authentification to work
git config --global user.email "bot@kpler.com"
git config --global user.name "Kpler Bot"

#
python tools/bump_version_number.py

# `$1` is set to keep the script generic. As long as you provide the repository
# name as first argument, you can use (and share) it everywhere that has the
# bump script defined above
# NOTE a sane possible fallback: `basename $PWD`
git remote add pushorigin "https://${KPLER_GITHUB_PUSH_TOKEN}@github.com/Kpler/$1.git"

git push --tags pushorigin master:master
