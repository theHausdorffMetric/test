"""Exact Earth AIS data collection
==================================

This spider leverages ExactAIS product from ExactEarch website to scrape
vessels info and position. Their API exposes 3 types of request:

    - Latest Vessel Information
    - Historical Vessel Tracks (not implemented)
    - Historical Vessel Points (not implemented)


Latest Vessel Information
~~~~~~~~~~~~~~~~~~~~~~~~~

To update the vessel data, run in a shell::

    $ cd <path to lng-scrapers project>
    $ scrapy crawl ExactAIS -o - -t jl \
        -a apikey=<exact earth api key> \
        -a limit=10                  # if you dont want the whole fleet \
        -a "imo=11111,222222,44444"  # to force specific vessels\
        -a window=2 \                # only updates at most 2 hours old
        -a gc=true                   # experimental memory optimization

Typical production setup tends to be::

    $ scrapy crawl ExactAIS -o /tmp/eais-crawl.jl -t jl \
        -a apikey=<exact earth api key>

Specifying some of these parameters affects how the scraper works :

- An `mmsi` will trigger a single shot for this vessel

- `bulk` and `paginate` will ask for bulk of data by ranges of ordered mmsi,
  and then filter them against our own `vessel_list`. This is kind of a hack to
  paginate the results and be nice/efficient with the API.

- With none of them, the scraper will use Kpler `vessel_list` and crawl 1
  `mmsi` at a time

In addition the scraper tries to only ask for data updated since the last time
+ `window` hours (if provided).

"""

from __future__ import absolute_import, unicode_literals
import datetime as dt
import gc

import dateutil.parser
from scrapy.http import Request

from kp_scrapers.lib.static_data import fetch_kpler_fleet
from kp_scrapers.lib.utils import grouper
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.ais import AisSpider
from kp_scrapers.spiders.ais.exactearth import constants
from kp_scrapers.spiders.bases.persist import PersistSpider

from .api import ECQLRequestFactory
from .parser import parse_response


def is_eligible(vessel):
    return constants.PROVIDER_ID in vessel.get('providers', []) and vessel.get('status') == 'Active'


class ExactAISSpider(AisSpider, PersistSpider):
    """Kind-of-REST API query to retrieve positions for a given vessel.

    cf: http://www.exactearth.com/products/exactais

    """

    name = 'ExactAIS'

    version = '1.1.0'
    provider = 'EE'
    produces = [DataTypes.Ais, DataTypes.Vessel]

    spider_settings = {
        'DATADOG_CUSTOM_METRICS': [
            # we usually want to monitor the quantity of data found and the performance
            'item_scraped_count',
            'response_received_count',
            # logs can be a good indicator of something going wrong
            'log_count/WARNING',
            'log_count/ERROR',
            # ee spider has the bad idea to often fail on too much memory used
            'memusage/max',
        ]
    }

    DEFAULT_MATCH_KEY = 'imo'
    # reasonable number advised by them
    DEFAULT_BATCH = 200

    def __init__(self, apikey, *args, **kwargs):
        """Arguments *need* to be passed so that the spider can run."""
        super(ExactAISSpider, self).__init__(*args, **kwargs)

        self.apikey = apikey
        # support one-shot vessel scraping
        self.match_key = kwargs.get('match', self.DEFAULT_MATCH_KEY)
        # comma seperated list of `match_key` keys
        self.forced = kwargs.get('force')

        # support n vessels query
        self.fleet_limit = int(kwargs.get('limit', constants.NO_FLEET_LIMIT))
        self.fleet_batch = int(kwargs.get('batch', self.DEFAULT_BATCH))

        # optimize calls limiting them to latest updates (in minutes)
        # if not given, it will use last time call
        self.time_window = kwargs.get('window')
        if self.time_window:
            self.time_window = dt.datetime.utcnow() - dt.timedelta(minutes=int(self.time_window))

        # feature flag mainly useful to compare efficiency of solutions
        self.optimize_memory = kwargs.get('gc') is not None

    def get_fleet(self):
        """Build a list of vesssels given the information available, i.e.:


            - list of given ids (depending on they key matched)
            - entire internal fleet with 'EE' as a provider

        Returns:
            (list): List of vessels

        """
        if self.forced:
            given_keys = self.forced.split(',')
            self.logger.debug('scenario: forced list of {}: {}'.format(self.match_key, given_keys))
            for each in given_keys:
                yield each
        else:
            # by default
            self.logger.debug('scenario: default (no mmsi, internal fleet)')
            for i, vessel in enumerate(fetch_kpler_fleet(is_eligible)):
                if i < self.fleet_limit and vessel.get(self.match_key):
                    yield vessel[self.match_key]

    def since_last_call(self):
        """Compute timedelta between now and the last time the scraper started."""
        last_run_start = self.persisted_data.get('spider_start')
        return dateutil.parser.parse(last_run_start)

    def _build_request(self, ids, last_call):
        # give it a bit of marge to make sure we catch everything
        last_call = last_call - dt.timedelta(minutes=constants.WINDOW_TOLERANCE)
        return ECQLRequestFactory(self.apikey).since(last_call).match(self.match_key, ids).build()

    @staticmethod
    def default_window(lag=5):
        return dt.datetime.now() - dt.timedelta(minutes=5)

    def start_requests(self):
        window_request = self.time_window or self.since_last_call() or self.default_window()
        self.logger.info(
            'requesting feed window={}h fleet={} vessels'.format(window_request, self.fleet_limit)
        )

        fleet_chunks = grouper(self.get_fleet(), self.fleet_batch)
        for i, vessels in enumerate(fleet_chunks):
            # remove None added to fill the group
            vessels = [x for x in vessels if x is not None]
            if (i + 1) * self.fleet_batch >= self.fleet_limit and i != 0:
                self.logger.info(
                    'reached requested fleet limit of {}, aborting.'.format(self.fleet_limit)
                )
                break

            url = self._build_request(vessels, window_request)
            yield Request(url=url, callback=self.parse)

    def parse(self, response):
        """Wrap EE parsing logic for scrapy."""
        if b'Exception' in response.body:
            self.logger.error('API call failed: %s', response.body)
            # doesn't mean we want to loose what other requests brought, move on
            return
        elif b'numberReturned="0"' in response.body:
            self.logger.warning('request didnt yield any result')
            # may be not a big deal, move on
            return

        try:
            for data in parse_response(response):
                yield data
                # we no longer need this object
                if self.optimize_memory:
                    del data
            if self.optimize_memory:
                gc.collect()
        except Exception as e:
            self.logger.warning('failed to handle response: %s', e)
