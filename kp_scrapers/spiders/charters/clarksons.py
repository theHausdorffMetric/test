# -*- coding: utf-8 -*-

"""Clarksons website scrapper.

Clarkson is selling fixture data (more generally shipping data), and won't sell
it to us. However, they are showing pieces of information on their website.  By
aggregating those pieces cleverly, we can rebuild a nice database.

Learn more: https://kpler1.atlassian.net/browse/KP-4599

"""

from __future__ import absolute_import
import json
import re

from scrapy.exceptions import CloseSpider
from scrapy.http import Request
from scrapy.spiders import CrawlSpider
import six
from six.moves.urllib import parse

from kp_scrapers.models.items import SpotCharter, TimeCharter, VesselIdentification
from kp_scrapers.spiders.charters import CharterSpider


BASE_URL = 'https://sin.clarksons.net/'
ROUTES_URL = {'grid': 'appselection/grid/', 'list_grid': 'Register/ListingGrid/'}
# The combination of tabs on the main page is linked to a single Id for
# requesting the data in the tables
URL_IDS = {
    'tanker_vessel': 176,
    'tanker_route': 149,
    'tanker_charterer_all': 165,
    'tanker_charterer_spot': 166,
    'tanker_charterer_time': 167,
    'dry_vessel': 189,
    'dry_route': 187,
    'dry_charterer_all': 196,
    'dry_charterer_spot': 197,
    # NOTE this is actually `period` but it looks like quivalent (to be
    # confirmed) and it gives us an easier `cli`
    'dry_charterer_period': 198,
    # NOTE no `route` tab, can we do it anyway ?
    'gaz_vessel': 164,
    'gaz_charterer_all': 159,
    'gaz_charterer_spot': 160,
    'gaz_charterer_period': 161,
}
SUPPORTED_FIXTURES = ['tanker', 'dry']
SUPPORTED_CHARTERS = ['all', 'time', 'spot']
DEFAULT_FIXTURE = 'tanker'
DEFAULT_CHARTER = 'spot'


def _spot_charter_to_uid(item):
    """Hash spot_charter.

    Must be identical for two fixtures with different charterer or route

    Args:
        spot_charter ():
        reported_date (): Reported date of fixtures, one vessel can only
                          has one reported per day

    Returns:
        str

    Example:
        >>> _spot_charter_to_uid({})
        >>> _spot_charter_to_uid({'X01_CVN': 'cvn'})
        >>> _spot_charter_to_uid({'FIX_DATE': 'date'})
        >>> _spot_charter_to_uid({'X01_CVN': 'cvn', 'FIX_DATE': 'date'})
        'cvndate'

    """
    if not item.get('X01_CVN') or not item.get('FIX_DATE'):
        return None

    return item.get('X01_CVN') + item.get('FIX_DATE')


def _url_factory(url_id, route_name, **opts):
    # either we research by documen id or by id
    is_doc = opts.get('is_doc', False)
    id_key = 'documentId' if is_doc else 'id'
    id_value = url_id if is_doc else URL_IDS[url_id]

    parameters = parse.urlencode(
        {
            id_key: id_value,
            'search': '',
            'take': opts.get('take', 1),
            'skip': 0,
            'page': 1,
            'pageSize': opts.get('take', 1),
        }
    )

    return '{}{}?{}'.format(BASE_URL, ROUTES_URL[route_name], parameters)


class ClarksonSpider(CharterSpider, CrawlSpider):
    """The best way to understand this spider is to read methods in the order.

    - Retrieving vessel ids
    - (yield) Retrieving routes (SC) infos
        - Retrieving routes (SC) infos
        - Retrieving charterer infos and cross with previous routes (SC)
    - (yield) Retrieving time charters

    """

    name = 'Clarksons'

    # opt-in our user-agent rotator middleware
    rotate_user_agent = True

    # Mapping for spot_charter
    sc_item_mapping = {
        'arrival_zone': 'arrival_zone',
        'departure_zone': 'departure_zone',
        'lay_can_end': 'FIX_LC_TO',
        'lay_can_start': 'FIX_LC_FROM',
        'rate_raw_value': 'DRATE',
        'rate_value': 'DRATE',
        'reported_date': 'FIX_DATE',
    }
    # Mapping for time_charter
    tc_item_mapping = {
        'charterer': 'charterer',
        'end_date': 'FIX_DATE_PERIOD_FINISH_MIN',
        # TODO: Rate is not accessible directly, find data to cross
        # 'rate_raw_value': 'DRATE',
        # 'rate_value': 'DRATE',
        'reported_date': 'FIX_DATE',
        'start_date': 'FIX_DELIVERY_FM',
    }

    vessel_mapping = {
        # 'internal_vessel_id': 'X01_CVN',
        'build_year': 'FIX_VSL_YOB',
        'dwt': 'FIX_VSL_DWT',
    }

    def __init__(self, **kwargs):
        super(ClarksonSpider, self).__init__(**kwargs)

        given_fixture = kwargs.get('fixture')
        given_charter = kwargs.get('charter')

        # TODO defaults are for no input, raise invalid cli if not supported
        self.charter_kind_target = (
            given_charter if given_charter in SUPPORTED_CHARTERS else DEFAULT_CHARTER
        )
        self.fixture_kind_target = (
            given_fixture if given_fixture in SUPPORTED_FIXTURES else DEFAULT_FIXTURE
        )

        self.vessels = {}
        self.items_with_vessel = {}

    def resource(self, target):
        if target in ['vessel', 'route']:
            return '{}_{}'.format(self.fixture_kind_target, target)
        elif target == 'charterer':
            return '{}_{}_{}'.format(self.fixture_kind_target, target, self.charter_kind_target)
        else:
            raise ValueError('{} is not a valid resource on Clarksons'.format(target))

    def start_requests(self):
        """In order to assign vessel_name to items using their internal ids,
        need to 'rebuild' vessel list.

        """
        self.logger.info('starting to load vessel ids (target={})'.format(self.charter_kind_target))
        yield Request(
            _url_factory(self.resource('vessel'), 'grid'), callback=self.parse_total_vessel_ids
        )

    def parse_total_vessel_ids(self, response):
        json_response = json.loads(response.body)
        total_records = json_response.get('TotalRecords')
        url = _url_factory(self.resource('vessel'), 'grid', take=total_records)

        return Request(url, callback=self.parse_vessel_ids)

    def parse_vessel_ids(self, response):
        self.logger.info('starting to parse vessel ids')
        # ~4k-5k vessels ftm in cache, tolerable
        json_response = json.loads(response.body)

        for document in json_response['RegisteredDocuments']:
            # `ArgumentValue` holds clarksons vessels' id
            self.vessels[document.get('ArgumentValue')] = {
                'name': re.sub(r'\([^)]*\)', '', document.get('DocumentTitle')).strip(),
                # not used: 'info': document.get('DocumentSummary'),
            }

        if self.charter_kind_target == 'spot':
            self.logger.info('done, scrapping spot charters')
            # Once vessels are saved with internal ids, next both requests are independant
            yield Request(
                _url_factory(self.resource('route'), 'grid'), callback=self.parse_total_record_route
            )
        elif self.charter_kind_target == 'time':
            self.logger.info('done, scrapping time charters')
            # Send a request on route to get the actual amount of routes.
            yield Request(
                _url_factory(self.resource('charter'), 'grid'),
                callback=self.parse_total_record_time_charter,
            )

    def parse_total_record_route(self, response):
        """Send a request to get all routes and meta infos.

        """
        json_response = json.loads(response.body)
        total_records = json_response.get('TotalRecords')
        url = _url_factory(self.resource('route'), 'grid', take=total_records)

        return Request(url, callback=self.parse_all_route)

    def parse_all_route(self, response):
        """Send request for each route to retrieve all associated fixtures.

        """
        json_response = json.loads(response.body)
        for document in json_response.get('RegisteredDocuments'):
            total_records = document.get('RecordCount')
            routes = document.get('ArgumentValue').split('-->')

            if len(routes) != 2:
                # Should never occurs with current format, see source ih happens
                continue

            departure_zone, arrival_zone = routes[0], routes[1]
            # IMPORTANT, Set priority=1 for nexts requests to ensure charterer
            # will be parse AFTER route
            route_url = _url_factory(
                document.get('DocumentId'), 'list_grid', take=total_records, is_doc=True
            )
            yield Request(
                route_url,
                callback=self.parse_route,
                priority=1,
                meta={'departure_zone': departure_zone, 'arrival_zone': arrival_zone},
            )

        # Once route data is complete, we retrieve charterer data
        # send a request on charterer to get the actual amount of charterers.
        yield Request(
            _url_factory(self.resource('charterer'), 'grid'),
            callback=self.parse_total_record_charterer,
        )

    def parse_route(self, response):
        """Parse fixture items and yield them.

        """
        try:
            json_response = json.loads(response.body)
        except ValueError as e:
            # it's possible we got a message like `browser not supported`, please upgrade.
            # Anyway without this data we cannot find anything relevant during
            # this run, fix the spider.
            raise CloseSpider(reason='failed to request routes: {}\n\n{}'.format(e, response.body))

        for item in json_response.get('listingData'):
            item['arrival_zone'] = response.meta['arrival_zone']
            item['departure_zone'] = response.meta['departure_zone']
            sc = self.parse_spot_charter_item(item)
            fixture_uid = _spot_charter_to_uid(item)

            if sc is None or fixture_uid is None:
                continue

            self.items_with_vessel[fixture_uid] = sc

    def parse_spot_charter_item(self, item):
        sc = SpotCharter()
        for key, value in six.iteritems(self.sc_item_mapping):
            sc[key] = item.get(value)

        sc['vessel'] = VesselIdentification()
        for key, value in six.iteritems(self.vessel_mapping):
            sc['vessel'][key] = item.get(value)

        sc['vessel']['name'] = self.vessels.get(item.get('X01_CVN'), {}).get('name')
        if sc['vessel']['name'] is None:
            self.logger.debug('vessel not found: {}'.format(item.get('X01_CVN')))
            return None

        return sc

    def parse_total_record_charterer(self, response):
        """Send a request to get all charterers and meta infos.

        """
        json_response = json.loads(response.body)
        total_records = json_response.get('TotalRecords')
        url = _url_factory(self.resource('charterer'), 'grid', take=total_records)

        yield Request(url, callback=self.parse_all_charterer)

    def parse_all_charterer(self, response):
        """Send request for each charterer to retrieve all associated fixtures.

        """
        json_response = json.loads(response.body)
        for document in json_response.get('RegisteredDocuments'):
            charterer = document.get('ArgumentValue')
            total_records = document.get('RecordCount')
            route_url = _url_factory(
                document.get('DocumentId'), 'list_grid', take=total_records, is_doc=True
            )
            yield Request(route_url, callback=self.parse_charterer, meta={'charterer': charterer})

    def parse_charterer(self, response):
        json_response = json.loads(response.body)

        for item in json_response.get('listingData'):
            fixture_uid = _spot_charter_to_uid(item)

            if fixture_uid not in self.items_with_vessel:
                self.logger.warning('unmatch charterer: {}'.format(fixture_uid))
                continue

            self.items_with_vessel[fixture_uid]['charterer'] = response.meta['charterer']
            sc = self.items_with_vessel[fixture_uid]
            self.items_with_vessel.pop(fixture_uid)

            yield sc

    def parse_total_record_time_charter(self, response):
        """Send a request to get all routes and meta infos.

        """
        json_response = json.loads(response.body)
        total_records = json_response.get('TotalRecords')

        return Request(
            _url_factory(self.resource('charterer'), 'grid', take=total_records),
            callback=self.parse_all_time_charter,
        )

    def parse_all_time_charter(self, response):
        """Send request for each route to retrieve all associated fixtures.

        """
        json_response = json.loads(response.body)
        for document in json_response.get('RegisteredDocuments'):
            charterer = document.get('ArgumentValue')
            total_records = document.get('RecordCount')
            route_url = _url_factory(
                document.get('DocumentId'), 'list_grid', take=total_records, is_doc=True
            )

            yield Request(
                route_url, callback=self.parse_time_charter, meta={'charterer': charterer}
            )

    def parse_time_charter(self, response):
        json_response = json.loads(response.body)
        for item in json_response.get('listingData'):
            item['charterer'] = response.meta['charterer']
            yield self.parse_time_charter_item(item)

    def parse_time_charter_item(self, item):
        tc = TimeCharter()
        for key, value in six.iteritems(self.tc_item_mapping):
            tc[key] = item.get(value)

        tc['vessel'] = VesselIdentification()
        for key, value in six.iteritems(self.vessel_mapping):
            tc['vessel'][key] = item.get(value)

        tc['vessel']['name'] = self.vessels.get(item.get('X01_CVN'), {}).get('name')
        if tc['vessel']['name'] is None:
            self.logger.warning('vessel not found: {}'.format(item.get('X01_CVN')))
            return None

        return tc
