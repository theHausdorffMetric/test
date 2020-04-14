# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import datetime as dt
import itertools
import json
import string

import scrapy
from six.moves import map
from six.moves.urllib import parse

from kp_scrapers.spiders.contracts import ContractSpider


DOMAIN = 'unipass.customs.go.kr'

# Characters 5 to 8, will always be in this list. It can be amended over time
# (it's made of agent codes that were seen dealing LNG, LPG, Crude or CPP
# cargoes)
AGENT_CODES = set(
    [
        'ASAN',
        'ASAC',
        'BHAT',
        'BHAB',
        'BMCL',
        'BYSP',
        'CSSi',
        'CBMC',
        'CBCi',
        'DDSP',
        'DKSP',
        'DRPT',
        'ESLJ',
        'ESKY',
        'EMSK',
        'FDCL',
        'FUJi',
        'FWMT',
        'GMSL',
        'GNMC',
        'GRTS',
        'HCSP',
        'HHGP',
        'HiCo',
        'HNUL',
        'HUBA',
        'HYSP',
        'iNUi',
        'JTMP',
        'JTNS',
        'KoEL',
        'KSSC',
        'LBRS',
        'KUMX',
        'LNCM',
        'KLCP',
        'KCAS',
        'LUMA',
        'NGSC',
        'oDFB',
        'oDFJ',
        'oSiL',
        'PANo',
        'SACL',
        'SBSP',
        'SBSH',
        'SEPL',
        'SiTK',
        'SJSS',
        'SLiN',
        'SoCo',
        'SoSP',
        'SMUS',
        'SSSW',
        'TAWU',
        'TFiV',
        'SYLK',
        'TMCJ',
        'TKZ0',
        'TRAS',
        'UMSP',
        'UNAM',
        'UPMC',
        'UTSH',
        'VoKR',
        'WELC',
        'WJDS',
        'WoMi',
        'WLSC',
        'WLHN',
        'YKLL',
        'YKSH',
        'YoUN',
    ]
)


def combine_chars(length, universe=string.ascii_uppercase):
    """Generate all the combinations of {length} letters.

    """
    universe = [universe] * length
    rand_chars = itertools.product(*universe)
    return list(map(''.join, rand_chars))


def gen_search_terms(year, agent):
    IMPORT_CODE = 'i'
    agent_list = [agent] if agent else AGENT_CODES
    for agent_code in agent_list:
        # TODO find patterns in those 4 last letters to reduce possibilities
        for next_two in combine_chars(2, string.ascii_uppercase + string.digits):
            for final_two in combine_chars(2, string.digits):
                yield year + agent_code + next_two + final_two + IMPORT_CODE


class KoreaSpider(ContractSpider, scrapy.Spider):

    # not an ideal name but we already have a KoreaCustoms that scrapes statistics
    name = 'KoreaVesselCustoms'
    version = '0.1.1'
    provider = None

    allowed_domains = [DOMAIN]
    base_url = 'https://{}/csp/myc/bsopspptinfo/cscllgstinfo/ImpCargPrgsInfoMtCtr/retrieveImpCargPrgsInfoLst.do'.format(  # noqa
        DOMAIN
    )

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Accept-Encoding': 'gzip, deflate, br',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Host': DOMAIN,
        'isAjax': 'true',
        'Origin': 'https://{}'.format(DOMAIN),
        'Referer': 'https://{}/csp/index.do'.format(DOMAIN),
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',  # noqa
        'X-Requested-With': 'XMLHttpRequest',
        'Cache-Control': 'no-cache',
    }

    # TODO add datadog
    spider_settings = {'DOWNLOAD_TIMEOUT': 900}

    def __init__(self, agent=None, year=None):
        self.search_year = year or dt.datetime.utcnow().strftime('%y')
        self.search_agent = agent

    def search_body(self, search_term, bulk_size=10000):
        # NOTE the `property` makes it easy to evolve if tomorrow we want to
        # customize parameters without too much API change
        return parse.urlencode(
            {
                'firstIndex': 0,
                'page': 1,
                'pageIndex': 1,
                'pageSize': bulk_size,
                'pageUnit': bulk_size,
                'recordCountPerPage': bulk_size,
                'qryTp': 1,
                'mblNo': '',
                'hblNo': '',
                'blYy': '',
                'cargMtNo': search_term,
            }
        )

    def start_requests(self):
        # generate all 2 letters combinations to cover everything
        for term in gen_search_terms(self.search_year, self.search_agent):
            # search terms are formatted as {YY}{agency}{...} so we try all the
            # possible first string characters for the given year
            body = self.search_body(term)
            yield scrapy.Request(
                self.base_url, method='POST', headers=self.headers, body=body, meta={'search': body}
            )

    def parse(self, response):
        payload = json.loads(response.body_as_unicode())

        if payload.get('error') == 'true':
            # NOTE `message` is in Korean so we get a unicode error trying to print it
            self.logger.error(
                'received an error message {} ({})'.format(
                    payload['errortype'], response.meta['search']
                )
            )
        elif payload['count'] == 0:
            self.logger.debug('no data received: {} ({})'.format(payload, response.meta['search']))
        else:
            for item in payload['resultList']:
                yield {
                    'mblNo': item['mblNo'],
                    'hblNo': item['hblNo'],
                    'cargMtNo': item['cargMtNo'],
                    'consignee': item['cnsiNm'],
                    'consigneeAdress': item['cnsiAddr'],
                    'product': item['prnm'],
                    'loadPort': item['ldpr'],
                    'arrivalDate': item['etprDt'],
                    'dischargePort': item['unldPortAirptNm'],
                    'vesselName': item['sanm'],
                    'ntprConm': item['ntprConm'],
                    'ntprAddr': item['ntprAddr'],
                    'quantity': item['ttwg'],
                    'kg': item['kg'],
                    'volume': item['cargMsrm'],
                    'shipAgent': item['trcoNm'],
                }
