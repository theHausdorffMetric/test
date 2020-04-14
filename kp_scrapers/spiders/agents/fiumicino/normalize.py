import datetime as dt  # noqa
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_apply, may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.
        REQUIRES ALL of the following fields:
        - port_name
        - provider_name (as defined in `BaseEvent`)
        - reported_date
        - vessel

    REQUIRES AT LEAST ONE of the following fields:
        - arrival
        - berthed
        - departure
        - eta

    Optional fields:
        - cargoes OR cargo
        - installation
        - next_zone
        - berth
        - shipping_agent

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping(reported_date=parse_date(raw_item['reported_date'])))
    # priority rules for berthed date: new etb >> etb >> berthed
    item['berthed'] = (
        item.pop('new etb', None) or item.pop('etb', None) or item.pop('berthed', None)
    )
    item['arrival'] = item.pop('arrival', None) or item.pop('arrived eta', None)
    item.pop('etb', None), item.pop('etc', None), item.pop('new etb', None), item.pop(
        'arrived eta', None
    )

    if not (item.get('eta') or item.get('arrival') or item.get('berthed') or item.get('departure')):
        logger.info(f'Item contains no valid dates: {raw_item}')
        return None

    cargoes = item.pop('cargoes', None)
    for cargo in cargoes:
        item['cargo'] = cargo
        yield item


def normalize_vessel_name(raw_vessel):
    vessel_name = re.sub(r'[\(\[].*?[\)\]]', '', raw_vessel)
    vessel_name = re.sub(r'[^a-zA-Z0-9 -]', '', vessel_name)
    return may_strip(vessel_name)


def normalize_date(raw_date, reported_date, event):
    """Normalize raw date to an ISO8601 compatible timestamp.

    Raw dates can come in different formats( see comment in code)

    Raw dates as given by the source do not provide month and/or year, so this info needs to be
    inferred from the reported date, together with the associated vessel movement type.

    A naive approach using month at scraping time may lead to inaccurate dates, especially
    during the rollover period for each month/year where dates from the previous and current
    month can appear together in the same table.

    To solve this, we compare if date is in the past or future:
        - For 'arrived', 'etb', 'berthed', 'new etb' vessel movements,
          reject if computed date is in the future.
          Then, return computed date adjusted 1 month/year prior.
        - For other vessel movements, reject if computed date is in the past.
          Then, return the computed date adjusted 1 month/year into the future.


    Args:
        raw_date (str): raw date string
        reported_date (dt.datetime): used to infer missing month/year in the dates
        event (str): used to determine if we should increment month/year when inferring

    Returns:
        str: ISO8601 formatted timestamp

    Examples:
        >>> normalize_date('25/02 2230', dt.date(year=2020, month=2, day=26), 'etb')
        '2020-02-25T00:00:00'
        >>> normalize_date('26/0902', dt.date(year=2020, month=2, day=26), 'arrived')
        '2020-02-26T00:00:00'
        >>> normalize_date('12/02/20 PM', dt.date(year=2020, month=2, day=18), 'arrived')
        '2020-02-12T00:00:00'
        >>> normalize_date('25/02(TBC)', dt.date(year=2020, month=2, day=18), 'eta')
        '2020-02-25T00:00:00'
        >>> normalize_date('26/AM (SUSPENDED)', dt.date(year=2018, month=11, day=7), 'etb')
        '2018-10-26T00:00:00'

    """
    regularize_month = False
    # remove the alphabetic part of the date
    raw_date = re.split(r'([^0-9\/]+)', raw_date)[0]
    split_date = raw_date.split('/')

    # if raw_date in the form : '18/PM' > remove alphabetical > raw_date = '18/' (DD/)
    if len(split_date) > 1 and split_date[1] == '':
        day, month, year = split_date[0], reported_date.month, reported_date.year
        regularize_month = True

    # if raw_date in the form for ex: '12/02/20PM' > ... > raw_date = '12/02/20' (DD/MM/YY)
    elif len(split_date) == 3:
        day, month, year = split_date[0], split_date[1], reported_date.year
    elif len(split_date) == 2:

        # if raw_date in the form for ex: '15/02AM' > ... > raw_date = '15/02' (DD/MM)
        if len(split_date[1]) == 2:
            day, month, year = split_date[0], split_date[1], reported_date.year

        # if raw_date in the form for ex: '26/0902' (DD/hhmm)
        if len(split_date[1]) == 4:
            day, month, year = split_date[0], reported_date.month, reported_date.year
            regularize_month = True

        # if raw_date in the form for ex: '21/02 2234' (DD/MM hhmm)
        if len(split_date[1]) == 7:
            day, month, year = split_date[0], split_date[1][0:2], reported_date.year
    else:
        return None

    # regularize month for the case where we get month infos from the reported_date
    if regularize_month:
        if event in ['arrived', 'etb', 'berthed', 'new etb'] and reported_date.day < int(day):
            month = reported_date.month - 1
        if event not in ['arrived', 'etb', 'berthed', 'new etb'] and reported_date.day > int(day):
            month = reported_date.month + 1

    # handle year rollover scenarios
    if int(month) == 12 and reported_date.month == 1:
        year -= 1
    elif int(month) == 1 and reported_date.month == 12:
        year += 1

    return may_apply(f'{day}/{month}/{year}', to_isoformat)


def normalize_cargo(raw_cargo):
    """Normalize cargo string into 1 or more products and corresponding quantities

    Examples:
        >>> list(normalize_cargo('disch abt mt 30000 unleaded'))
        [{'product': 'unleaded', 'volume': '30000', 'movement': 'discharge', 'volume_unit': 'tons'}]
        >>> list(normalize_cargo('disch abt 90000 jet a1'))
        [{'product': 'jet a1', 'volume': '90000', 'movement': 'discharge', 'volume_unit': 'tons'}]

    Args:
        raw_cargo (str): raw cargo data

    Yields:
        Dict[str, str]:

    """

    if 'load' in raw_cargo:
        movement = 'load'
    elif 'disch' in raw_cargo:
        movement = 'discharge'
    for cargo in [may_strip(product) for product in raw_cargo.split('+')]:
        quantity = re.search(r'([0-9]+)', cargo).group(0)
        product = may_strip(
            re.sub(r'\bmt\b|\babt\b|\bloading\b|\bdisch\b|\bdischarging\b|\b\d+\b', '', cargo)
        )
        yield {
            'product': product,
            'volume': quantity if quantity else None,
            'movement': movement,
            'volume_unit': Unit.tons,
        }


def field_mapping(**kwargs):
    return {
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'berth': ('berth', None),
        'vessel': ('vessel', lambda x: {'name': normalize_vessel_name(x)}),
        'arrived': ('arrival', lambda x: normalize_date(may_strip(x), **kwargs, event='arrived')),
        'arrived eta': (
            'arrived eta',
            lambda x: normalize_date(may_strip(x), **kwargs, event='arrived'),
        ),
        'eta': ('eta', lambda x: normalize_date(may_strip(x), **kwargs, event='eta')),
        'etb': ('etb', lambda x: normalize_date(may_strip(x), **kwargs, event='etb')),
        'etc': ('etc', lambda x: normalize_date(may_strip(x), **kwargs, event='etc')),
        'ets': ('departure', lambda x: normalize_date(may_strip(x), **kwargs, event='ets')),
        'berthed': ('berthed', lambda x: normalize_date(may_strip(x), **kwargs, event='berthed')),
        'new etb': ('new etb', lambda x: normalize_date(may_strip(x), **kwargs, event='new etb')),
        'ops': ('cargoes', lambda x: list(normalize_cargo(x))),
    }
