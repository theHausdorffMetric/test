#! /usr/bin/env bash

# unofficial strict mode
set -eo pipefail

# build first pdf container

# then build scrapinghub-ready image
docker build --rm \
  -t kpler/scrape \
  --build-arg "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
  --build-arg "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
  .

# finally wrap kp-scrapers with scrapinghub-like environment
docker build --rm -t kpler/scrape:shub tools/build

# TODO upload on AWS
