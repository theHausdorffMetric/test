# The following URI is requested to be able to schedule scrapers not parsing web pages using Scrapy.
#
# There are a few considerations to picking a suitable URI:
#   - long downtimes are not acceptable at all, for the continued operation of spiders
#   - resource must be available for reading near 100% of the time
#   - resource must be available anywhere on all systems our repo runs on
#
# Given these conditions, `/etc/hosts` was chosen since it exists on all linux distros,
# even scrapinghub's custom spider container for running spiders.
BLANK_START_URL = 'file:///etc/hosts'
