#!/usr/bin/env bash

# activate virtualenv - all the steps since it is mostly used by the crontab
export WORKON_HOME='/home/kpler/.virtualenvs'
source /usr/share/virtualenvwrapper/virtualenvwrapper.sh
workon kp-scrapers

# -- scrapinghub consistency
# make sure we don't pretend to be on scrapnighub. While we want spiders mostly
# to behave like on scrapinghub, sometimes envs differ in important ways.
# Dotscrapy for example currently doesn't work on the server but does on
# scrapinghub.
unset SHUB_JOBKEY
export SCRAPY_LOG_LEVEL="DEBUG"
export SCRAPY_PROJECT_ID="321191"  # this is the actual scrapinghub production env id
export SCRAPY_SPIDER_ID="85"       # that's MarineTrafficAIS2

# -- extensions
export DATADOG_API_KEY="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
export DATADOG_APP_KEY="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
export AWS_ACCESS_KEY_ID="xxxxxxxxxxxxxxxxxxxx"
export AWS_SECRET_ACCESS_KEY="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
# bringin back addons data to our own bucket
export ADDONS_S3_BUCKET="scrapinghub-kpler-addons"
# NOTE doesn't seem to be taken into effect
# FIXME version 0.3.0 doesn't seem to support Python 3
export DOTSCRAPY_ENABLED="false"

# -- Kpler stuff
# used to define where items will be uploaded on s3 (default to `pre-production`)
export KP_ENV="production"

{
  cd /home/kpler/kp-scrapers || echo "fail to cd in kp-scrapers, did you clone it?"

  export SCRAPY_JOB="${SCRAPY_PROJECT_ID}/${SCRAPY_SPIDER_ID}/$RANDOM"

  scrapy crawl "${@}" >> /home/kpler/logs/vais-scraper.log 2>&1
}
