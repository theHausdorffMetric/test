# -*- coding: utf-8 -*-

"""Private settings for Scrapy.

`kp-scrapers` tries to import `local_settings` so add this directory to
`$PYTHONPATH` and it will include the settings defined here.

This is meant to be private so keep this file out of the repository, so that it
will be still operational after a new clone (git will ignore it if you keep it
in the repository but you don't want to reset it everytime)

        $ mkdir ../__private
        $ # the file name is mandatory since it's used in `kp_scrapers/settings.py`
        $ cp local_settings.tpl ../__private/local_settings.py
        $ # make it available
        $ export PYTOHNPATH=$PWD/../__private:$PYTHONPATH

"""

# import os

# SCRAPY_PROJECT_ID = 'local'

# define AWS standard credentials, used essentailly for S3 access (requests
# cache, vessels list, items export, ...)
# also it's not recommended to set them up in your `.profile`, it's a common
# practice to have credentials store (temporarily) in the environment (see
# 12factors app for some rational about this)
# AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
# AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

# no need to specify the crawlera url, we use the default one
# CRAWLERA_ENABLED = False
# CRAWLERA_APIKEY = 'xxxx'

# Datadog extension settings
# it also work for local environment if you set the correct SCRAPY_PROJECT_ID
# DATADOG_API_KEY = '*****'
# DATADOG_APP_KEY = '*****'

# define Google credentials for accessing any Google API, especially Drive/Docs/Sheets/Slides
# GOOGLE_DRIVE_BASE_FOLDER_ID = 'xxxx'
# GOOGLE_DRIVE_DEFAULT_USER = 'someone@company.com'
# GOOGLE_DRIVE_PRIVATE_KEY = '-----BEGIN PRIVATE KEY-----\nxxx=\n-----END PRIVATE KEY-----\n'  # noqa
# GOOGLE_DRIVE_PRIVATE_KEY_ID = 'xxxx'

# define Google Drive item storage settings.
# This is mandatory to activate the DriveRawStorage pipeline.
# Despite its name, the string supplied should be purely the raw export folder's ID
# KP_DRIVE_FEED_URI = '****'
# NOTE disabled by default as we don't want to clutter Drive storage
# KP_DRIVE_ENABLED = False

# Define GMail credentials required for accessing email reports.
# GMAIL_USER = 'person@kpler.com'
# GMAIL_PASS = '****'
# To customise the client secrets, tokens and IDs, you will need to authenticate via
# client secrets and get the respective key values from the JSON credentials provided by Google
# See the following for what the values mean:
# - https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
# You can obtain a `client_secret.json` the Google developers console by creating an
# OAuth2 client ID credential:
# - https://console.developers.google.com/apis/credentials
# After obtaining your client secret file, run the quickstart code to generate your own
# access token and other keys:
# - https://developers.google.com/drive/v3/web/quickstart/python#step_3_set_up_the_sample
# GMAIL_CLIENT_SECRET = '****'
# GMAIL_ACCESS_TOKEN = '****'
# GMAIL_CLIENT_ID = '****'
# GMAIL_REFRESH_TOKEN = '****'
# GMAIL_TOKEN_EXPIRY = '****'

# define local `tabula.jar` path, required if testing pdf spiders
# TABULA_JAR_PATH = 'your_local_tabula_path'

# define credentials for Kpler Excel API
# KP_API_BASE = '****'
# KP_API_EMAIL = '****'
# KP_API_PASSWORD = '****'

# usually activated only on Scrapinghub but you may want to have it for
# sateteful test or if playing with `persistent` misinx
# DOTSCRAPY_ENABLED = True

# proxy all HTTP requests through this endpoint thanks to the local Proxy
# middleware. Useful if you want to use a transparent proxy (for fixed IP or
# spoofing location), or hide spider behavior behind a "polipo + tor" setup
# HTTP_PROXY = 'http://127.0.0.1:8123'
# but without this setting to true, the value above is ignored
# HTTP_PROXY_ENABLED = True

# define airtable API key
# AIRTABLE_API_KEY = '****'

# needed to talk to Slack api
# SLACK_TOKEN = '****'
# uncomment to customize the default behavior
# SLACK_CHANNEL = '#crew-data-sourcing'

# SENTRY_DSN = 'https://******@sentry.io/261345'
# SENTRY_THRESHOLD = 'warning'

# define default behaviour upon receiving 4xx/5xx http errors
# ON_HTTP_ERROR = 'finish'  # 'finish' or 'continue'
