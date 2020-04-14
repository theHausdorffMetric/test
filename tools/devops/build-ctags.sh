#! /usr/bin/env bash

# stolen from https://tbaggery.com/2011/08/08/effortless-ctags-with-git.html

set -e

#PATH="/usr/local/bin:$PATH"
dir="$(git rev-parse --show-toplevel)"

trap 'rm -f "$dir/$$.tags"' EXIT

__build_tags() {
	ctags -L - -f"$dir/$$.tags" --recurse=yes \
	  --langmap=python:.py --python-kinds=-iv \
		--exclude=tests --exclude=tools/ --exclude=githooks/ --exclude=scheduling/ --exclude=code_review/ --exclude=doc/ --exclude=build --exclude=dist \
    "$1"
}

echo "[ kp-scrapers ] re-indexing project tags"
git ls-files | __build_tags "kp_scrapers"

echo "[ kp-scrapers ] saving to $dir/tags"
mv "$dir/$$.tags" "$dir/tags"
