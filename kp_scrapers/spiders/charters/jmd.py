# -*- coding: utf-8 -*-

"""日本海事新聞 電子版 - The Japan Maritime Daily
https://www.jmd.co.jp/

Parser that retrieves charters from the 'markets' page.

"""

import datetime as dt
import random
import re

from scrapy import Request, Spider
from scrapy.http import FormRequest
from six.moves import range

from kp_scrapers.lib.utils import mean
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.utils import parse_arrival_zones


PATH_TO_TANKER_DIV = '//div[contains(@class, "page-market--tanker")]'
PATH_TO_TANKER_TABLE = (
    PATH_TO_TANKER_DIV + '//tr[local-name()="tr" and (descendant::*[local-name()="td"])]'
)
SINGLE_DATE_RGX = re.compile(r"(\d{1,2})\s*/\s*(\d{1,2})")
PERIOD_RGX = re.compile(r"(\d{1,2})\s*/\s*(\d{1,2})-\s*(\d{1,2})")
CHARTERER_RGX = re.compile(r"^(.*)-\s*(CORR|FLD|REPL|UPDATE|RPTD|RCNT|RPLC)\s*$")


def parse_vessel_name(name):
    """Parse
     - (01) MARAN CASTOR
     - AMCL TBN
     - DUBAI GLAMOUR O/O

    Args:
        name (string): the raw name.

    Returns:
        A string corresponding to the clean name if there is a name.
        If invalid name is provided in input, None.

    """
    name = name.replace('O/O', '').strip()
    if name.startswith('('):
        return name.split(')')[1].strip()
    elif name == u'\xa0':
        return None
    else:
        return name


class UnhandledDataFormatException(Exception):
    pass


def parse_dwt(dwt):
    """Parse
     - \xa0 => None
     - 270/280 => 275
     - 270 => 270

    Args:
        dwt (string): The raw dwt

    Returns:
        An int if the format is handled, None if unaccurate dwt input.

    Raises:
        ValueError if the dwt can not be casted form string to int.

    """
    try:
        if dwt != u'\xa0':
            dwts = [int(d) for d in dwt.split("/")]
            # value was originally in kilotons
            return int(mean(dwts) * 1000)

    except ValueError as e:
        raise UnhandledDataFormatException(
            'this dwt format is not handled by function parse_dwt'
            ' and raises ValueError: {}'.format(e)
        )


def parse_loading_date(loading_date):
    """Parse:
     - 3 / 18-20
     - 3/19
     - 4 / ELY

    Args:
        loading_date (string): The raw input date.

    Returns:
        dt.date, dt.date objects corresponding to the input
        if data format handled else None, None.

    """
    lay_can_start = None
    lay_can_end = None

    def to_datetime(m, d):
        # The year could be around the 1st of January if this Japanese
        # website computes the date based on a JST datetime (UTC+9).
        return dt.datetime(dt.datetime.utcnow().year, int(m), int(d))

    period = PERIOD_RGX.match(loading_date)
    if period:
        month, day_1, day_2 = period.groups()
        lay_can_start = to_datetime(month, day_1)
        lay_can_end = to_datetime(month, day_2)
    else:
        single_date = SINGLE_DATE_RGX.match(loading_date)
        if single_date:
            month, day = single_date.groups()
            lay_can_start = lay_can_end = to_datetime(month, day)

    return lay_can_start.isoformat(), lay_can_end.isoformat()


def parse_contract(charterer):
    """
    Parse:
    - \xa0 => (None, None)
    - UNIPEC
    - CSSSA - FLD
    - MERCURIA - REPL
    - CHEMCHINA - UPDATE
    - BRIGHTOIL - CORR
    - BRIGHTOIL - RPTD
    - BRIGHTOIL - RCNT
    - BRIGHTOIL - RPLC

    Args:
        charterer (string): the raw string containing the charterer's name
        and status infos for the charter.

    Returns:
       string, string objects if data format handled, else None, None.

    """
    with_status = CHARTERER_RGX.match(charterer)
    if u'\xa0' in charterer:
        return None, None
    elif with_status:
        name, status = with_status.groups()
        name = name.strip()
    else:
        status = ''
        name = charterer

    mapping = {
        'FLD': 'Failed',
        'REPL': 'Replaced',
        'UPDATE': 'Updated',
        'CORR': 'Fully Fixed',
        '': 'Fully Fixed',
        'RPTD': 'Fully Fixed',
        'RCNT': 'Fully Fixed',
        'RPLC': 'Replaced',
    }

    return name, mapping[status]


def parse_areas(areas):
    """
    Parse
    KUWAIT / EAST
    SERIA + / THAI  #  Partial loading. Not yet supported
    B.URIP/TJP+THAI  #  Partial loading. Not yet supported

    Args:
        areas (string): the raw string containing departure and arrival areas.

    Returns:
        string, string objects if the data format is handled else None, None.

    """
    try:
        departure_zone, arrival_zone = areas.split('/')
        departure_zone = departure_zone.split('+')[0].strip()
        arrival_zone = arrival_zone.split('+')[0].strip()
        arrival_zone = parse_arrival_zones(arrival_zone)
        return departure_zone, arrival_zone
    except ValueError:
        return None, None


def parse_columns(columns):
    vessel_name, dwt, loading_date, areas, rate_value, contract = columns

    # build Vessel sub-model
    vessel = {'name': parse_vessel_name(vessel_name), 'dwt': parse_dwt(dwt)}

    lay_can_start, lay_can_end = parse_loading_date(loading_date)
    charterer, status = parse_contract(contract)
    departure_zone, arrival_zone = parse_areas(areas)
    return {
        "vessel": vessel,
        "lay_can_start": lay_can_start,
        "lay_can_end": lay_can_end,
        "departure_zone": departure_zone,
        "arrival_zone": arrival_zone,
        "charterer": charterer,
        "status": status,
        "rate_value": rate_value,
    }


@validate_item(SpotCharter, normalize=True, strict=False)
def validate_spot_charter(spot_charter, **kwargs):
    spot_charter.update(**kwargs)
    return spot_charter


class JMDSpider(CharterSpider, Spider):
    """
    日本海事新聞 電子版 - The Japan Maritime Daily
    """

    name = 'JMD'
    provider = 'JMD'
    version = '0.1.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    login_url = 'https://www.jmd.co.jp/login.php?ref=%2Fpage%2Fmarket.php'
    logout_url = 'https://www.jmd.co.jp/logout.php?gateway=out&ref=%2F'
    market_url = 'https://www.jmd.co.jp/page/market.php'

    start_urls = [login_url]

    credentials = [
        # ID, pwd
        ('Wb03c3167', '17KZ625d'),  # pioup390@gmail.com
        ('Xg03c3372', '17VM621sqwop'),  # paul@yopmail.com
        ('Yj03c3404', '17DF607b'),  # john.ouipi@yopmail.com
        ('Ya03c3405', '17NL924r'),  # gen.doe@yopmail.com
    ]

    def __init__(self, **kwargs):
        self.is_logged = False
        super(JMDSpider, self).__init__(**kwargs)

    def parse(self, response):
        if not self.is_logged:
            return self.login()
        else:
            return response

    def get_credentials(self):
        """
        Select randomly a login/password. Do not return the same couple twice.
        """
        if not hasattr(self, 'credentials_indexes'):
            self.credentials_indexes = set(range(0, len(self.credentials)))

        if not self.credentials_indexes:
            raise RuntimeError('Not enough credentials to be able to login to the JMD website')

        index = random.sample(self.credentials_indexes, 1)[0]
        self.credentials_indexes.remove(index)

        return self.credentials[index]

    def login(self):
        uid, pwd = self.get_credentials()
        self.logger.info("Login with the '{}' account".format(uid))
        return FormRequest(
            self.login_url,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            cookies={
                'REGCODE': '0a63acdb44aa224eebbed21693711043',
                'SCODE': '35926d4c8d636ae240814f41f5d313adb1fe73093c06e364e3',
            },
            formdata={
                'uid': uid,
                'pwd': pwd,
                'ref': '/page/market.php',
                'login': u'ログイン',
                'memory': '1',
            },
            method='POST',
            callback=self.after_login,
        )

    def after_login(self, response):
        if '/login.php' in response.url:
            # We should be redirected to market.php
            self.logger.warning("Login failed")
            return self.login()
        else:
            self.is_logged = True
            return self.parse_market(response)

    def parse_market(self, response):
        count_items = 0
        reported_date = response.xpath(PATH_TO_TANKER_DIV + '/p[1]/text()').extract_first()
        reported_date = re.search(r'\d+/\d+/\d+', reported_date).group(0)
        reported_date = dt.datetime.strptime(reported_date, '%Y/%m/%d').strftime('%d %b %Y')
        for table_row in response.xpath(PATH_TO_TANKER_TABLE)[1:]:
            columns = [i.extract() for i in table_row.xpath('.//td/text()')]
            try:
                charter_props = parse_columns(columns)
            except Exception as e:
                self.logger.warning(u'Unable to parse: {}. MESSAGE: {}'.format(columns, e))
                continue
            charter_props['reported_date'] = reported_date
            if not (
                charter_props['charterer']
                or not (charter_props['departure_zone'] or charter_props['arrival_zone'])
            ):
                # No data
                # Note: it could be a boat purchase or a time charter.
                # TODO: save it.
                self.logger.warning(u'Unable to parse [{}]'.format(u','.join(columns)))
                continue
            yield validate_spot_charter(charter_props, provider_name=self.provider)
            count_items += 1
        self.logger.info('Parsed {} items'.format(count_items))
        yield Request(url=self.logout_url, callback=lambda n: None)
