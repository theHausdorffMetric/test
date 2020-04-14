"""Vessel Finder AIS data collection
=================================

This spider implements a client for VesselFinder's REST API.

API documentation: https://api.vesselfinder.com/docs/vesselslist.html


Fleet management
----------------

VesselFinder does not provide any API to manage your fleet. Everything
is done via their support desk.


Position data retrieval
-----------------------

Position retrieval as explained above is done against a fleet. The
fleet seems to be related to credentials as there API does not expose
any fleet selection mecanism.

Thus spider invocation is simply::

  $ cd <path to lng-data project>/spiders
  $ scrapy crawl -a user_key=<API-USER-KEY> --loglevel=INFO VesselFinderApi


"""

from __future__ import absolute_import, unicode_literals

from scrapy.exceptions import CloseSpider
from scrapy.http import Request
from scrapy.spiders import Spider
from six.moves.urllib import parse

from kp_scrapers.lib.date import str_month_day_time_to_datetime, to_isoformat
from kp_scrapers.lib.parser import serialize_response
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.items import VesselPositionAndETA
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.ais import AisSpider, safe_heading, safe_imo


SUPPORTED_AIS_TYPE = 'T-AIS'
PROVIDER_ID = 'VF API'

KEY_MAP = {
    'A': ('master_dimA', int),
    'B': ('master_dimB', int),
    'C': ('master_dimC', int),
    'CALLSIGN': ('master_callsign', str),
    'COURSE': ('position_course', int),  # -- 5
    'D': ('master_dimD', float),
    'DRAUGHT': ('position_draught', float),
    'HEADING': ('position_heading', safe_heading),  # CVD: Heading value seems to be fine
    'IMO': ('master_imo', safe_imo),
    'LATITUDE': ('position_lat', float),  # -- 10
    'LONGITUDE': ('position_lon', float),
    'MMSI': ('master_mmsi', str),
    'NAME': ('master_name', str),
    'SPEED': ('position_speed', float),
    'TIMESTAMP': ('position_timeReceived', lambda x: to_isoformat(x, dayfirst=False)),
    'TYPE': ('master_shipType', int),
    'NAVSTAT': ('position_navState', int),
    # TODO: Check if the VF logic to compute ETA is good enough for us. If yes, we might
    # TODO: use ETA directly (and not ETA_AIS anymore)
    # 'ETA': ('nextDestination_eta', lambda x: to_isoformat(x, dayfirst=False)),
    'ETA_AIS': ('nextDestination_eta', str_month_day_time_to_datetime),
    'DESTINATION': ('nextDestination_destination', str),  # -- 19
}


def normalize(raw_item):
    item = VesselPositionAndETA(
        provider_id=PROVIDER_ID,
        aisType=SUPPORTED_AIS_TYPE,
        nextDestination_aisType=SUPPORTED_AIS_TYPE,
        position_aisType=SUPPORTED_AIS_TYPE,
        **map_keys(raw_item['AIS'], KEY_MAP),
    )

    if not item['nextDestination_eta']:
        # NOTE same is done on the ETL actually
        del item['nextDestination_eta']
        del item['nextDestination_destination']
        del item['nextDestination_aisType']

    return item


class VesselFinderApi(AisSpider, Spider):
    """VesselFinder API client.

    Args:
        user_key(str): as provided by the contract
        max_age(str): The maximum age of the returned positions (in minutes)

    """

    name = 'VesselFinderApi'
    version = '2.1.0'
    # NOTE ideally we would prefer to keep VF, but DB is king
    provider = 'VF API'
    produces = [DataTypes.Ais, DataTypes.Vessel]

    # generic url, minus the parameters
    _API_URL_TPL = "https://api.vesselfinder.com/{method}?{params}"
    # VesselFinder supports different aPI calls but we only pay for this one, a
    # fix list of vessels' positions
    # doc: https://api.vesselfinder.com/docs/vesselslist.html
    _API_METHOD = 'vesselslist'

    def __init__(self, apikey, max_age=None):
        # note on semantic: it's a `user key` for VF but it's good to keep the
        # same cli terminology across our spiders
        self._request_params = {'userkey': apikey}
        if max_age:
            self.logger.info('limiting request to last {} minutes'.format(max_age))
            self._request_params['interval'] = max_age
        # we can also customize response format (json or xml) but  json is already the default

    def start_requests(self):
        params = parse.urlencode(self._request_params)
        uri = self._API_URL_TPL.format(method=self._API_METHOD, params=params)

        yield Request(uri, callback=self.retrieve_positions)

    @serialize_response('json')
    def retrieve_positions(self, messages):
        # on error we receive a dict, but successful messages are a list
        if isinstance(messages, dict) and messages.get('error'):
            self.logger.error('unexpected API error: {}'.format(messages['error']))
            self.process_error(messages['error'])

        return [normalize(ais_msg) for ais_msg in messages]

    @staticmethod
    def process_error(error_msg):
        # TODO are there other error responses for which we want to send a custom job outcome?
        # send custom job outcome for datadog service checks
        if 'Expired account' in error_msg:
            raise CloseSpider('banned')
        else:
            raise RuntimeError(error_msg)
