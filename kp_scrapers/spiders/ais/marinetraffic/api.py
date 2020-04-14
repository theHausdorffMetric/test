"""MT API helpers and common bits.

Ideally this class could serve any use case but its current implementation in
spiders make use of Scrapy requests scheduler and therefore needs Scrapy
runtime.

"""

from __future__ import absolute_import
import functools
import logging

from scrapy.exceptions import CloseSpider
from scrapy.http import Request
import six


BASE_URL = 'https://services.marinetraffic.com'
# unique source ID for downstream identification
MAX_MT_TIMESPAN = 60  # minutes
PREFERED_PROTOCOL = 'jsono'
API_VERSION = 8
# MarineTraffic feeds we subscribe too
# splitted into because they acknowledged not being able to support one large
# fleet and it appeared to be the most practiacl solution for both parties
FLEETS = {
    # DEPRECATED
    # 'MT_API_LNG': '927309'
    # 9000 vessels wide fleet (original merge of legacies)
    'MT_API': 927308,
    # 6000 vessels created to track the coal fleet
    'MT_API_5000': 1208175,
    # extension of cpp and coal fleets including smaller vessels
    'MT_API_SMALL': 1374916,
}
# as supported by Marine Traffic API
# - Simple messages contain only positions.
# - Extended messages contains some static data about the vessel and on its
#   current voyage (next destination, etc.)
# cf. http://www.marinetraffic.com/en/ais-api-services/documentation/api-service:27
MESSAGE_TYPES = ['simple', 'extended']

logger = logging.getLogger(__name__)


def check_errors(func):
    """Abort spider on API error response.

    NOTE: This decorator is expected to be used after MarineTraffic response was
    serialized to `json`.

    """

    @functools.wraps(func)
    def _inner(klass, data):
        if isinstance(data, list):
            # nothing to do, getting positions successfully returns a list
            return func(klass, data)

        if data.get('errors'):
            # NOTE could try to reach fleet['errors'][i]['detail']
            err = data['errors']
            raise CloseSpider("Data retrieval error: {}".format(err))

        if isinstance(data.get('DATA'), dict):
            # it seems MT sends different format of data depending on the fleet
            # you request. Instead of a list of vessels we get a dictionnary
            # with MT ID as a key. Since we don't care about this, we normalise
            # back the format to a convenient list
            data['DATA'] = list(data['DATA'].values())

        return func(klass, data)

    return _inner


class MTClient(object):
    def __init__(self, fleet_name, **apikeys):
        self.fleet_name = fleet_name
        self.fleet_id = FLEETS[fleet_name]

        # required to request positions (which doesn't need keys above, but is
        # specific to the fleet)
        self.poskey = apikeys.get('poskey')
        # fleet agnostic
        self.getkey = apikeys.get('getkey')
        self.setkey = apikeys.get('setkey')

    @staticmethod
    def build_url(method, token, **opts):
        url_tpl = '{base}/api/{method}/{token}/{params}'

        # common for all calls
        opts['protocol'] = PREFERED_PROTOCOL
        if method == 'exportvessels':
            # for whatever reason this is necessary on this method since November 2018
            opts['v'] = API_VERSION

        params = '/'.join(['{}:{}'.format(k, v) for k, v in six.iteritems(opts)])

        return url_tpl.format(base=BASE_URL, method=method, token=token, params=params)

    def register(self, imo, callback, **meta):
        opts = {'imo': imo, 'fleet_id': self.fleet_id, 'active': 1}
        # add a bit of context for the callback
        meta.update(opts)
        return Request(
            self.build_url('setfleet', self.setkey, **opts), callback=callback, meta=meta.copy()
        )

    def remove(self, imo, callback, **meta):
        # note: the operation is omnipotent on MT side
        # TODO support deactivate
        opts = {'imo': imo, 'fleet_id': self.fleet_id, 'delete': 1}
        meta.update(opts)
        url = self.build_url('setfleet', self.setkey, **opts)
        return Request(url, callback=callback, meta=meta.copy())

    def positions(self, callback, timespan=MAX_MT_TIMESPAN, msgtype='simple'):
        """Fetch fleet positions.

        Documentation: https://www.marinetraffic.com/en/ais-api-services/documentation/api-service:ps03/_:9ddb73ed41060f83944f51d4f1e3e313  # noqa

        """
        opts = {'timespan': timespan, 'msgtype': msgtype}
        url = self.build_url('exportvessels', self.poskey, **opts)
        return Request(url, callback=callback)

    def fetch_fleet(self, callback):
        """Fetch fleet details.

        Documentation: https://www.marinetraffic.com/en/ais-api-services/documentation/api-service:pu03  # noqa

        """
        logger.info('fetching fleet #{}'.format(self.fleet_id))
        # TODO check errors
        # NOTE parse here metadatas and datas ?
        url = self.build_url('getfleet', self.getkey, fleet_id=self.fleet_id)
        return Request(url, callback=callback)
