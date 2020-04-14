import os

from kp_scrapers.settings.utils import env_is_true, is_shub_env


# include memory statistics in scrapy stats
MEMUSAGE_ENABLED = 1

# Save source files retrieves and cache requests/responses
DOWNLOADER_MIDDLEWARES = {
    # Uncomment to register crawlera and safely test tricky spiders locally.
    # You will also need to uncomment and fill CRAWLERA_ENABLED and
    # CRAWLERA_API below, although it is recommended to use a `local_settings`
    # as descried at the end of this file
    'scrapy_crawlera.CrawleraMiddleware': 300,
    # deactivate default and instead rotate fake user agents in case website
    # use it to block requests
    # FIXME Set it to None cause HTTP errors (400, 403),
    # 'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    # custom user-agent rotator
    # `scrapy_fake_user_agent` gets its user agents list from the internet and
    # sometimes the endpoint is not available... Or worse it could be
    # deprecated in the future. So more efficient and future-proof is to
    # copy/paste this list under Kpler roof
    'kp_scrapers.middlewares.user_agent.RotateUserAgentMiddleware': 310,
    # force requests to go through a transparent proxy. Only acitvated if
    # `HTTP_PROXY_ENABLED` setting is set
    'kp_scrapers.middlewares.proxy.ProxyMiddleware': 320,
    # store raw requests/response for drop-in replacement of Scrapy cache
    # Right before HttpCompressionMiddleware, so when the request comes back it
    # is ungziped by HttpCompressionMiddleware before being stored in S3
    # learn more: https://github.com/kpler/scrapy-history-middleware
    'history.middleware.HistoryMiddleware': 589,
    # allows for dynamic retrieval of data from javascript heavy pages
    # or to work around bot detection algorithms
    'scrapy_splash.SplashCookiesMiddleware': 723,
    'scrapy_splash.SplashMiddleware': 725,
    # enforce geolocation checks on spider IP addresses
    # note that this does not work with crawlera, and you might want to disable it then
    'kp_scrapers.middlewares.geolocation.GeolocationMiddleware': 999,
}

SPIDER_MIDDLEWARES = {
    # allow addition of fields based on machine state
    'scrapy_magicfields.MagicFieldsMiddleware': 100,
    # don't tolerate invalid http answers
    'kp_scrapers.middlewares.http_error.HTTPErrorMiddleware': 305,
    # disable built-in http error middleware in favour of ours
    'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': None,
}

ITEM_PIPELINES = {
    # extend items with project and context information
    'kp_scrapers.pipelines.context.EnrichItemContext': 200,
    # pretty print item stats once done
    'kp_scrapers.pipelines.reporter.ReportStats': 201,
    # store raw (i.e. dict) items on S3
    'kp_scrapers.pipelines.s3.S3RawStorage': 300,
    # store raw (i.e. csv) items on Google Drive
    'kp_scrapers.pipelines.drive.DriveStorage': 400,
    # upload item stats to redshift
    'kp_scrapers.pipelines.redshift.RedshiftStats': 406,
    # notify relevant channel that a spider finished or crashed
    # NOTE this is supposed to be inside `EXTENSIONS`, but it turns out spider attributes
    # are not passed properly there, so we need to place it here to be able to have
    # export urls be exposed to the notify extensions
    'kp_scrapers.extensions.notify.NotifyMiddleware': 410,
}

# uncomment and modify DSN to enable `sentry` spider middleware and start
# sending exceptions on Sentry
# SENTRY_DSN = 'https://******@sentry.io/261345'

MAGIC_FIELDS = {  # The fields that we add to every item using machine state:
    "sh_item_time": "$isotime",  # Machine time of scraping
    "sh_spider_name": "$spider:name",  # The name of the spider
    "sh_job_id": "$jobid",  # The Scrapinghub jobid
    "sh_job_time": "$jobtime",  # Machine time of job launch
}

# extend metadatas collected in magicfields.
# those, like url, can be quite heavy for items, so it's switched off by
# default. Wheneveryou find it useful, aither export the environment variable
# (for development) or set it permanently in the scraper settings on
# Scrapinghub
if env_is_true('DEBUG'):
    MAGIC_FIELDS.update({'request_url': '$response:url', 'request_status': '$response:status'})

EXTENSIONS = {
    # To create stats like item_scraped_count, response_received_count, item_dropped_count...
    'scrapy.extensions.corestats.CoreStats': 1,
    # Needed to allow interpolation of job_id and project_id in settings string,
    # for example in S3 bucket name high priority, before Feed exporter that have a
    # priority of 0
    'scrapyjobparameters.extension.JobParametersExtension': -2,
    # For Datadog alerts (ours).
    'scrapydatadog.extension.DatadogExtension': 2,
    # To remember some vars between two crawls.
    'scrapy_dotpersistence.DotScrapyPersistence': 0,
    # capture exceptions and send them on Sentry
    'kp_scrapers.extensions.sentry.SentryErrorTracker': 600,
}

# Use DotScrapy Persistence if running on Scrapinghub.
# Report to spiders/persist_data_manager for main use case
# or http://help.scrapinghub.com/scrapy-cloud/addons/dotscrapy-persistence-addon.
DOTSCRAPY_ENABLED = is_shub_env()
# DotScrapy configuration
# ADDONS_AWS_ACCESS_KEY_ID = "ABC"
# ADDONS_AWS_SECRET_ACCESS_KEY = "DEF"
# ADDONS_AWS_USERNAME = "username"
ADDONS_S3_BUCKET = "scrapinghub-kpler-addons"

# S3 item storage settings. Activate feed export.
# We don't want to export result from local env
if is_shub_env() or os.getenv('FORCE_S3_STORAGE'):
    # This is mandatory to activate  the S3RawStorage pipeline and as you can
    # see in the template, it depends on other mandatory parameters set by our
    # job_parameters extension.
    #
    # Not that renaming `KP_RAW_FEED_URI` to
    # `FEED_URI` will cause the pipeline to be disabled, and instead the
    # built-in feed exporter will take over. Basically the same behaviour minus
    # item meta fields and metrics.  You can even get this behaviour on a
    # per-spider configuration
    #
    #       from kp_scrapers;settings import FEED_URI
    #
    # this layout de-clutter the root S3 namespace and follow the same path
    # style as data dispatcher output. It also abstracts the environmennt id so
    # we can recreate it without disrupting actual output target where workers
    # downstream are expecting to find the data.
    # Finally it allows a workflow where all pre-production spiders have an
    # existing target by default, while explicit settings is enforced for
    # production or custom setups (like kp-xxxx environments).
    #
    # the `stream` folder allows to keep different kind of data under the
    # spider namespace. This is directly usuable by the cache extension but let
    # the door open to whatever could come in the future
    #
    KP_RAW_FEED_URI = 's3://%(bucket)s/%(env)s/%(name)s/stream/%(time)s--%(name)s--%(job_id)s.jl'

# History middleware settings
HISTORY_BACKEND = 'history.storage.S3CacheStorage'
HISTORY_EPOCH = True
HISTORY_RETRIEVE_IF = 'history.logic.RetrieveNever'
HISTORY_SAVE_SOURCE = '{name}/cache/{time}__{jobid}'
HISTORY_STORE_IF = 'history.logic.StoreAlways'
# one need to set this on scrapinghub with the correct env id, like `FEED_URI`
# NOTE the good approach would be to patch the middleware and allow templated
# value
# HISTORY_S3_BUCKET = 'kp-datalake'
HISTORY_USE_PROXY = True
HTTPCACHE_IGNORE_MISSING = False

# scrapy-splash middleware settings
SPLASH_URL = 'http://scrapers-splash.prod.galil.io:8050'

# Datadog extension settings
# DATADOG_HOST_NAME = 'app.scrapinghub.com'  # (default)
DATADOG_METRICS_PREFIX = 'kp.sh.spiders.stats'
# as usual, secrets are expected to be set on SHUB interface
DATADOG_API_KEY = os.environ.get('DATADOG_API_KEY')
DATADOG_APP_KEY = os.environ.get('DATADOG_APP_KEY')
# allow extension deactivation on local env while keeping API keys ser
DATADOG_DISABLED = os.environ.get('DATADOG_DISABLED')

# secret, expected to be set on SHUB
# AWS_ACCESS_KEY_ID = 'xxxx'
# AWS_SECRET_ACCESS_KEY = 'xxxx'
