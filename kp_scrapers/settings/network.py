# Crawl responsibly by identifying yourself (and your website) on the user-agent
# deprecated now since we use random user agents
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'  # noqa

# If `USER_AGENT_ROTATION_ENABLED` == True, a random user agent below will be used
USER_AGENT_LIST = [
    USER_AGENT,
    'Mozilla/5.0 (compatible; MSIE 10.0; Macintosh; Intel Mac OS X 10_7_3; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0)',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',  # noqa
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:70.0) Gecko/20100101 Firefox/70.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:63.0) Gecko/20100101 Firefox/63.0',
    'Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)',
    'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.146 Safari/537.36',  # noqa
    'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:28.0) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2',  # noqa
    'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:28.0) Gecko/20100101 Firefox/28.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.146 Safari/537.36',  # noqa
    'Mozilla/5.0 (X11; Linux x86_64; rv:24.0) Gecko/20140205 Firefox/24.0 Iceweasel/24.3.0',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:23.0) Gecko/20100101 Firefox/23.0',
    'Opera/9.80 (X11; Linux i686; U; ru) Presto/2.8.131 Version/11.11',
]

# London based transparent proxy when sources are expecting a fixed ip
HTTP_PROXY = 'http://scrapers-proxy-euwest2.prod.galil.io:8050'

# enforce due diligence checks for spiders that require a fixed location
#   - disabled by default
#   - settings below are exhaustive
# GEOLOCATION_ENABLED = False
GEOLOCATION_STRICT = True  # force spider to exit if check fails
# GEOLOCATION_CITY = 'London, GB'
# GEOLOCATION_API_KEY = '*****'

# Crawlera Settings - it's disabled by default for sane local development but:
# - you can tweack that locally with the `local_settings` import below
# - secrets like `CRAWLERA_USER` are expected to be set on SHUB
# - it is set by default in the production environment and must be  disabled
#   per-spider if needed (I'm not sure this implicit approach is the most
#   intuitive neither the safer, to be challenged)
# no need to specify the crawlera url, we use the default one
CRAWLERA_ENABLED = False
# CRAWLERA_APIKEY = '******'

# stuff you can set from SH UI or `local_settings`
# https://blog.scrapinghub.com/2016/08/25/how-to-crawl-the-web-politely-with-scrapy/
# try to look more human
# HTTPCACHE_ENABLED = False
# CONCURRENT_REQUESTS = 1
# CONCURRENT_REQUESTS_PER_IP = 1
# To use if some websites ban crawlers based on there requests frequency.
# DOWNLOAD_DELAY = 0.25
# Scrapy Autothrottle Settings: Autothrottle makes it possible to adjust the downloading time.
# setting it up: https://media.readthedocs.org/pdf/scrapy/master/scrapy.pdf, section Autothrottle.
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 5
AUTOTHROTTLE_MAX_DELAY = 60  # If the server does not answer within 60s, send another request.
# AUTOTHROTTLE_TARGET_CONCURRENCY = 6  # Default is 1 aka no concurent requests.
