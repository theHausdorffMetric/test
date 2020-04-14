# -*- coding: utf-8 -*-

"""Abidjan

Website: http://www.portabidjan.ci/

"""

from __future__ import absolute_import, unicode_literals
from datetime import datetime, timedelta

from scrapy.http import Request
from six.moves import range, zip

import kp_scrapers.lib.utils as utils
from kp_scrapers.models.items import ArrivedEvent, BerthedEvent, EtdEvent, VesselIdentification
from kp_scrapers.models.utils import filter_item_fields
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


PAGE_TYPES = [u'in_berth', u'arrival', u'departure']
EVENTS = [BerthedEvent, ArrivedEvent, EtdEvent]

TITLES = [
    u'Navires à Quai'.encode('utf8'),
    u'Liste des arrivées'.encode('utf8'),
    u'Liste des départs'.encode('utf8'),
]

BERTH_KEYS = [
    # on the csv file from Tabula, the name of the column 'Nom navire'
    # is always truncated
    u'om navire',
    u'Entrée',
    u'Sortie',
    u'Quai/Infra',
    u'Provenance',
    u'Destination',
]

ARRIVAL_DEPARTURE_KEYS = [
    u'om navire',
    u'Arrivée/Départ',
    u'Quai/Infra',
    u'Provenance',
    u'Destination',
]

HEADER_MARKER = 'om navire'.encode('utf8')


def keys_map():
    return {
        u'om navire': ('name', lambda x: x),
        u'Arrivée/Départ': ('eta', lambda x: x),
        u'Quai/Infra': ('berth', lambda x: x),
        u'Entrée': ('berthed', lambda x: x),
        u'Sortie': utils.ignore_key('no matching field in PA Events'),
        u'Provenance': utils.ignore_key('no matching field in PA Events'),
        u'Destination': utils.ignore_key('no matching field in PA Events'),
    }


def add_keys(row):
    keys = BERTH_KEYS if len(row) == len(BERTH_KEYS) else ARRIVAL_DEPARTURE_KEYS
    return {k: v for k, v in zip(keys, row)}


def row_to_dict(row):
    return utils.map_keys(add_keys(row), keys_map())


def extract_pa_event(row):
    row_is_pa_event = None not in row
    return row_to_dict(row) if row_is_pa_event else None


class AbidjanInfo(object):
    def __init__(self, table):
        self.table = table
        self.row_is_interesting = False

    def _is_interesting_row(self, row):
        row_is_title = row[0] == TITLES
        row_is_headers = HEADER_MARKER in row[0]
        if row_is_headers:
            self.row_is_interesting = True
            return False
        elif row_is_title:
            self.row_is_interesting = False
            return False
        return self.row_is_interesting

    def __next__(self):
        row = next(self.table)
        if self._is_interesting_row(row):
            return extract_pa_event(row)
        return None

    def next(self):
        return self.__next__()

    def __iter__(self):
        return self


class AbidjanSpider(PortAuthoritySpider, PdfSpider):

    name = 'Abidjan'
    version = '1.0.0'
    provider = 'Abidjan'

    start_urls = [
        'http://www.portabidjan.ci/',
        'http://www.portabidjan.ci/sites/default/files/navires_a_quai-{}.pdf',
        'http://www.portabidjan.ci/sites/default/files/listes_des_arrivees-{}.pdf',
        'http://www.portabidjan.ci/sites/default/files/liste_des_departs-{}.pdf',
    ]

    def start_requests(self):
        t = datetime.now()
        range_days = 7
        # Generate a list of dates to complete url templates.
        # Some of these urls correspond to existing site pages and some others do not.
        # This issue is handled by Scrapy under the hood since we use the default
        # handle_httpstatus_list attribute
        date_list = (t - timedelta(days=x) for x in range(0, range_days))
        for date_ in date_list:
            for page_type in PAGE_TYPES:
                yield Request(
                    url=self.start_urls[1].format(date_.strftime('%d%m%y')),
                    meta={'updated_time': date_.strftime('%d/%m/%Y'), 'page_type': page_type},
                    callback=self.parse,
                )

    def parse(self, response):
        type_item_mapping = {k: v for k, v in zip(PAGE_TYPES, EVENTS)}
        page_type = response.meta['page_type']
        item = type_item_mapping[page_type]
        for info in self.extract_pdf_table(response, AbidjanInfo):
            if info is not None:
                info.update(
                    {
                        'vessel': VesselIdentification(name=info['name']),
                        'url': response.url,
                        'updated_time': response.meta['updated_time'],
                        # We do not know if the item is aberth, an eta or an etd,
                        # Thus we copy eta in etd before filtering them.
                        'etd': info.get('eta', None),
                    }
                )
                info = filter_item_fields(item, info)
                yield item(**info)
