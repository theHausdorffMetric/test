import os


# Define Google credentials for accessing any Google API, especially Drive/Docs/Sheets/Slides
GOOGLE_DRIVE_BASE_FOLDER_ID = None
GOOGLE_DRIVE_DEFAULT_USER = None
GOOGLE_DRIVE_PRIVATE_KEY = None
GOOGLE_DRIVE_PRIVATE_KEY_ID = None
GOOGLE_TEAM_DRIVE_ENABLED = True

# Define Google Drive item storage settings.
# This is mandatory to activate the DriveRawStorage pipeline.
# Despite its name, the string supplied should be purely the raw export folder's ID
KP_DRIVE_FEED_URI = '1nbp4SbyYK73aFmLNcTcAVhUMYsxmRJU3'
# NOTE disabled by default as we don't want to clutter Drive storage

# Define GMail credentials required for accessing email reports.
GMAIL_USER = None
GMAIL_PASS = None
# To customise the client secrets, tokens and IDs, you will need to authenticate via
# client secrets and get the respective key values from the JSON credentials provided by Google
# See the following for what the values mean:
# - https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
# You can obtain a `client_secret.json` from the Google developers console by creating an
# OAuth2 client ID credential:
# - https://console.developers.google.com/apis/credentials
# After obtaining your client secret file, run the quickstart code to generate your own
# access token and other keys:
# - https://developers.google.com/drive/v3/web/quickstart/python#step_3_set_up_the_sample
GMAIL_CLIENT_SECRET = None
GMAIL_ACCESS_TOKEN = None
GMAIL_CLIENT_ID = None
GMAIL_REFRESH_TOKEN = None
GMAIL_TOKEN_EXPIRY = None

# marks the email as seen after spider processes it
# this is added to allow multiple spiders to run on a single email
MARK_MAIL_AS_SEEN = True

# define airtable API key
AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY')

# notification channel for job completion
SLACK_CHANNEL = '#data-notifications'
SLACK_TOKEN = os.environ.get('SLACK_TOKEN')
NOTIFY_DEV_IN_CHARGE = None

# define credentials for Kpler Excel API
KP_API_BASE = None
KP_API_EMAIL = None
KP_API_PASSWORD = None
