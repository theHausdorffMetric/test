"""Spire AIS data collection
=========================

This spider implements a client for Spider's REST API.


Fleet management
----------------

Spire does not provide any API to manage your fleet. Everything is done via
their support desk.


Position data retrieval
-----------------------

Position retrieval as explained above is done against a fleet. The
fleet seems to be related to credentials as there API does not expose
any fleet selection mecanism.

Thus spider invocation is simply::

  $ cd <path to lng-data project>/spiders
  $ scrapy crawl SpireApi        \
       -a 'token=xxxxxxxxxxxxxxxxxxxx'  \
       -a 'limit=1000000'


JSON document format
~~~~~~~~~~~~~~~~~~~~

Spire API documention only provides fragements of the JSON response
sent and on has to reverse engineer the exact format from sample
code they provide or actual date received from them. At the time
this has been written, actual data feed were not available.

"""

from __future__ import absolute_import, unicode_literals
import base64
import datetime as dt
import re
import string
import sys

import Levenshtein
from scrapy.exceptions import CloseSpider
from scrapy.http import Request
from scrapy.spiders import Spider
import six

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.services.shub import SPIDER_DONE_STATE
from kp_scrapers.lib.static_data import fetch_kpler_fleet
import kp_scrapers.lib.utils as utils
from kp_scrapers.mixins.distribute import DivideAndConquerMixin
from kp_scrapers.models.ais import AisMessage
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.ais import AisSpider
from kp_scrapers.spiders.ais.spire import api as spireapi
from kp_scrapers.spiders.ais.spire.normalize import map_message_data, map_vessel_data


# provider Kpler id/shortname for downstream
PROVIDER_ID = 'SPIRE'
# TODO part of `business.py`
# as understood by Scrapy and our loaders
# unused: SUPPORTED_MSG_TYPES = (1, 2, 3, 18, 5, 24, 27)
# for now it's difficult to process more messages downstream
# see data dispatcher and ETL constraints for more information
DEFAULT_MESSAGES_LIMIT = 6000
# Spire response are limited by 100 items for paging so if you put a modulo of
# 100 like `50` we will wrongly intepreted the last page to have a next one
FLEET_BATCH = 90
# Spire responses often include AIS static data of irrelevant vessels despite
# asking explicity for a specific vessel IMO, so this is to try and match relevant vessels
MIN_MESSAGE_SIMILARITY = 0.80

MAPPERS = {'vessels': map_vessel_data, 'messages': map_message_data}


def is_eligible(vessel):
    """Only select Spire-tracked vessel.

    Args:
        vessel(dict): as defined by our static fleet

    Examples:
        >>> is_eligible({'providers': ['SPIRE', 'MT_API'], 'status': 'Active'})
        True
        >>> is_eligible({'providers': ['SPIRE', 'MT_API'], 'status': 'Under Construction'})
        True
        >>> is_eligible({'providers': ['MT_API'], 'status': 'Active'})
        False
        >>> is_eligible({'providers': ['SPIRE', 'MT_API'], 'status': 'Broken Up'})
        False

    """
    return PROVIDER_ID in vessel.get('providers', []) and vessel.get('status') in [
        'Active',
        'Under Construction',
    ]


def dest_is_unknown(vessel):
    """Check for ETA information.

    Args:
        vessel(Dict): raw vessel information

    Examples:
        >>> dest_is_unknown({})
        True
        >>> dest_is_unknown({'next_destination_eta': None, 'next_destination_destination': 'China'})
        True
        >>> dest_is_unknown({'next_destination_eta': '2018', 'next_destination_destination': 'China'})  # noqa
        False

    """
    return (
        vessel.get('next_destination_eta') is None
        or vessel.get('next_destination_destination', '') == ''
    )


def name_is_similar(name_1, name_2, threshold):
    """Naively tries to check if two vessel names are the same.

    Uses a combination of rules-based matching, and fuzzy string parsing:
        1. remove special designations (e.g. M.T., S/T)
        2. remove all whitespaces and punctuation
        3. lower case everything
        4. use Jaro-Winkler similarity matching

    See `tests.ais.spiders.spire.test_spider` for examples of potential raw vessel names.

    """
    if not name_1 or not name_2:
        return False

    # remove special designations
    MT_MV_PATTERN = r'^M[\./]?[TV][\.\s]+'
    ST_PATTERN = r'S/T$'
    name_1 = re.sub(MT_MV_PATTERN, '', name_1)
    name_1 = re.sub(ST_PATTERN, '', name_1)
    name_2 = re.sub(MT_MV_PATTERN, '', name_2)
    name_2 = re.sub(ST_PATTERN, '', name_2)

    # remove whitespace and transform to lower case
    name_1 = may_strip(name_1).replace(' ', '').lower()
    name_2 = may_strip(name_2).replace(' ', '').lower()
    # remove punctuation
    name_1 = name_1.translate(str.maketrans('', '', string.punctuation))
    name_2 = name_2.translate(str.maketrans('', '', string.punctuation))

    # matching with Jaro-Winkler similarity
    # prefix weight chosen to prevent false matches
    score = Levenshtein.jaro_winkler(name_1, name_2, 1 / 50)

    return score >= threshold


def compute_since(**delta):
    """Compute since date and encode it for Spire.

    Args:
        delta(dict): time granularity as understood by timedelta

    """
    since_date = dt.datetime.utcnow() - dt.timedelta(**delta)
    return base64.encodestring(str(since_date))


def reduce_fleet_on(key, fleet):
    return [v.get(key) for v in fleet]


# TODO move this in lib
def bool_flag(raw_scrapy_flag):
    """Cast Scrapy cli argument to boolean flag.

    Examples:
        >>> bool_flag(None)
        False
        >>> bool_flag('true')
        True
        >>> bool_flag('y')
        True
        >>> bool_flag('foo')
        False

    """
    if not isinstance(raw_scrapy_flag, str):
        # probably `None`, which probably means it was not provided so `False`
        # seems to be an educated guess
        return False
    return raw_scrapy_flag.lower() in ['true', 'y', 'yes']


def is_integer_like(candidate):
    try:
        int(candidate)
        return True
    except (TypeError, ValueError):
        return False


class SpireApi(AisSpider, DivideAndConquerMixin, Spider):
    """Spire API recursive crawler.

    As long as the API sends data with paging indication, the crawler will
    perform requests on Spire API.
    Last call timestamp is recorded between runs so that we only ask new data.

    More information: https://spire.com/contact/developer-portal/?access=true#/index

    """

    name = 'SpireApi'

    version = '3.1.0'
    provider = PROVIDER_ID
    produces = [DataTypes.Ais, DataTypes.Vessel]

    # go easy on the API - we easily end up rate limited
    download_delay = 5.0
    spider_settings = {'CONCURRENT_REQUESTS_PER_DOMAIN': 1}

    def __init__(self, token, api, query_by='imo', **kwargs):
        super().__init__(**kwargs)

        # used to only yield one position per vessel (imo being the id)
        self._cache = set()
        self.token = token

        self.api = api
        # crash on purpose if `api` is invalid
        self.mapper = MAPPERS[api]

        self.skip_validation = bool_flag(kwargs.get('skip_validation'))

        self.batch_size = int(kwargs.get('batch', FLEET_BATCH))
        self.query_by = query_by
        self.fleet = list(fetch_kpler_fleet(is_eligible))
        self.vessel_ids = kwargs.get(query_by) or reduce_fleet_on(query_by, self.fleet)
        if isinstance(self.vessel_ids, six.string_types):
            self.vessel_ids = self.vessel_ids.replace(' ', '').split(',')

        self.message_similarity = float(kwargs.get('message_similarity', MIN_MESSAGE_SIMILARITY))

        slice_size = kwargs.get('slice_size')
        if slice_size:
            self.logger.info(f"slicing scraping scope to {slice_size}")
            self.vessel_ids = self.divide_work(self.vessel_ids, int(slice_size))

        # TODO on `message type`, use since with last run date
        # self.forced_since = compute_since(minutes=int(since)) if since else None

        # filter/limit messages
        # we use `since` for cli because from user point of view it's the same thing.
        # But programmaticaly it's not the `since` used by Spire pagination. instead, it's
        # the filter available to the `messages` api
        self.window = kwargs.get('since')  # in minutes (default will be set by Spire to 3h)
        self.messages_limit = int(kwargs.get('limit', DEFAULT_MESSAGES_LIMIT))

        self.messages_counter = 0

    def start_requests(self):
        """Run first request based on optional last run timestamp."""
        # retrieve the last request time as it was advertised by Spire API
        # `None` is fine, we will call the API without the parameter
        # since = self'imo': self.imos.forced_since or self.persisted_data.get('last_cursor')

        for page, fleet_partials in enumerate(utils.grouper(self.vessel_ids, self.batch_size)):
            # mmsi are sometimes
            opts = {self.query_by: ','.join([v for v in fleet_partials if is_integer_like(v)])}
            if self.window:
                # opts['received_before'] = dt.datetime.utcnow().isoformat()
                opts['received_after'] = (
                    dt.datetime.utcnow() - dt.timedelta(minutes=int(self.window))
                ).isoformat()

            if self.api == 'messages':
                # they also expose `static` types but that doesn't bring
                # latitude, longitude, although it does fetch imos, ...
                opts['msg_description'] = 'position'

            # note on sorting: default messages api will sort by timestamp in
            # ascending order, which is what we want

            # NOTE testing `vessels` resource
            yield Request(
                spireapi.make_request_url(self.api, **opts),
                headers=spireapi.headers(self.token),
                callback=self.on_positions,
                meta={'opts': opts},
            )

    def _too_much_items(self):
        return self.messages_counter > self.messages_limit

    def validate_vessel(self, item):
        """Check mandatory fields and item <-> vessel matching."""
        # vessel = utils.lookup_by('imo', item['master_imo'], self.fleet)
        if item.get('vessel').get('imo'):
            vessel = utils.lookup_by('imo', item.get('vessel')['imo'], self.fleet)
        else:
            vessel = utils.lookup_by('mmsi', item.get('vessel')['mmsi'], self.fleet)

        if vessel is None:
            raise KeyError(f"vessel not found in fleet: {item.get('vessel')['mmsi']}")

        # validate a few details
        assert item.get('position').get('lon') is not None, "No longitude provided"
        assert item.get('position').get('lat') is not None, "No latitude provided"
        # prevent inconsistent AIS signals from being passed to downstream
        #
        # NOTE may cause some vessels to not be scraped by SPIRE due to
        # differences in actual name and AIS name, but this is something we are
        # willing to do to improve overall data quality since a lot of false positions
        # we are getting from SPIRE come from irrelevant/outdated signals being provided
        # even though the requests explicitly call for a specific IMO and timeframe.
        assert name_is_similar(
            vessel['name'], item.get('vessel').get('name'), self.message_similarity
        ), f"Inconsistent vessel name: {vessel['name']} <-> {item.get('vessel').get('name')}"

        # then enrich with the data we know (yes that's cheating)
        if vessel['imo'] != item.get('vessel').get('imo'):
            item.get('vessel')['imo'] = vessel['imo']
        # name already matched and filtered above
        item.get('vessel')['name'] = vessel['name']
        if vessel['call_sign'] != item.get('vessel').get('call_sign'):
            item.get('vessel')['call_sign'] = vessel['call_sign']

        # check if we already saw this vessel. It should only happen on
        # the `messages` api and it's pretty safe to drop the data.
        # indeed we request Spire feed quite frequently and getting 100
        # positions for a vessel over a window of 3 minutes is useless
        # (and will be discarded by data dispatcher)
        # NOTE pollute warning logs - deactivating for now
        # assert vessel['imo'] not in self._cache, ""
        # self._cache.add(vessel['imo'])

        # NOTE that we don't actually need to return the item since mutating
        # the original dict is reflected outside the scope of the function
        # that's not great but what's the point of fooling ourselves and hiding
        # it behind a useless dict deepcopy or returned value

    @validate_item(AisMessage, normalize=True, strict=True, log_level='error')
    def on_positions(self, raw_response):
        response = spireapi.ResponseFromScrapy(raw_response)

        if response.has_next_page and False:
            # the implementaion is buggy but the way we currently request data
            # shouldn't get us here
            raise NotImplementedError("pagination is going under fixing")
            # FIXME using the `vessels` API we should actually never get there
            # and if we do `since` is tailored to `messages` API - `vessels`
            # should use `next`
            try:
                # move the cursor
                # self.persisted_data['last_cursor'] = response.paging['since']
                yield Request(
                    response.next_page(self.api, **raw_response.meta['opts']),
                    headers=spireapi.headers(self.token),
                    callback=self.on_positions,
                    meta=raw_response.meta,
                )
            except ValueError:
                err = 'Output may be incomplete because of the following error.'
                self.logger.exception(err)

        # so far so good, let's parse the response
        for item in response.data:
            mapped_item = self.mapper(item)

            item = {
                'vessel': {
                    'name': mapped_item.get('master_name'),
                    'imo': mapped_item.get('master_imo'),
                    'mmsi': mapped_item.get('master_mmsi'),
                    'vessel_type': mapped_item.get('master_shipType'),
                    'call_sign': mapped_item.get('master_callsign'),
                    'flag_name': mapped_item.get('master_flag'),
                },
                'position': {
                    'draught': mapped_item.get('position_draught'),
                    'lat': mapped_item.get('position_lat'),
                    'lon': mapped_item.get('position_lon'),
                    'speed': mapped_item.get('position_speed'),
                    'course': mapped_item.get('position_course'),
                    'ais_type': mapped_item.get('position_aisType'),
                    'received_time': mapped_item.get('position_timeReceived'),
                    'heading': mapped_item.get('position_heading'),
                },
                'reported_date': mapped_item.get('master_timeUpdated'),
                'provider_name': PROVIDER_ID,
                'ais_type': mapped_item.get('aisType'),
                'message_type': mapped_item.get('message_type'),
                'next_destination_eta': mapped_item.get('nextDestination_eta'),
                'next_destination_ais_type': mapped_item.get('aisType'),
                'next_destination_destination': mapped_item.get('nextDestination_destination'),
            }

            if not self.skip_validation:
                # Complete and validate item - Spire data is quite raw and
                # messy, don't bother the ETL with too raw/unreliable
                # information
                try:
                    self.validate_vessel(item)
                except (AssertionError, KeyError):
                    # NOTE stats? logging is too verbose
                    # NOTE it seems we're loosing ton of data here - imo being None
                    self.logger.warning(
                        "skipping vessel: {} // because: {}".format(item, sys.exc_info()[1])
                    )
                    continue

            # NOTE not sure how necessary that is
            if dest_is_unknown(item):
                item.pop('nextDestination_eta', None)
                item.pop('nextDestination_destination', None)
                item.pop('nextDestination_aisType', None)
                item.pop('nextDestination_timeUpdated', None)

            self.messages_counter += 1
            yield item

            # NOTE could we get the data from self.crawler.stats.spider_stats ?
            if self._too_much_items():
                # Spire continuously stream messages as long as there are
                # available. However we don't want to ingest them forever, nor
                # can we manage to send millions of them down the data
                # pipeline.
                self.logger.warning('reached messages limit, aborting crawler')

                # save current position to resume at the right place next time
                # NOTE currently deactivated
                # self.persisted_data['last_cursor'] = response.paging['since']

                # Since this issue mostly emerges when we stoped crawling
                # regurlarly the API (not expected behavior) and that other
                # providers are filling the data, the least worst strategy is
                # to just give up and abort the spider. Passing
                # `reason=SPIDER_DONE_STATE` will tell the data dispatcher it can
                # fetch the data we stored so far.
                raise CloseSpider(reason=SPIDER_DONE_STATE)


class SpireStream(SpireApi):
    """Identical Spider...

    rational: we need to keep a spider named SpireApi running for the snapshot
    loader to work (it searches for this explicit name on scrapinghub). And yet
    we want to release in parallel an updated version. And easy and dirty way
    of doing so is running what looks lie 2 different spiders.

    """

    name = 'SpireStream'
