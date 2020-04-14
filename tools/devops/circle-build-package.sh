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

# build package and upload to private pypi index
# sensitive variables `PYPI_{HOST, USERNAME,PASSWORD}` are expected to be set
# from your CI provider who probably handles secrets storage
cat > ~/.pypirc << EOF
[distutils]
index-servers = pypi-private

[pypi-private]
repository=${PYPI_HOST}
username=${PYPI_USERNAME}
password=${PYPI_PASSWORD}
EOF

# generate spiders meta datas. The file may often change and generate huge diff
# for no real values. Yet embedding it in the package allows for nice caching,
# especially for APIS using it.
scrapy describe --silent --filter enabled:true --export kp_scrapers/meta.json

python setup.py sdist upload -r pypi-private
