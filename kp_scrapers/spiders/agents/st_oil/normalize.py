import datetime as dt  # noqa
import logging

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_apply, may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping(reported_date=parse_date(raw_item['reported_date'])))

    # get the right date based on priority: berthed >>> etb >>> arrived >>> eta
    item['eta'] = item.pop('etb', None) or item.pop('eta', None)
    if not (item.get('eta') or item.get('arrival') or item.get('berthed')):
        logger.info(f'Item contains no valid dates: {raw_item}')
        return None

    # each table row may contain multiple cargoes, hence we iterate over each one
    cargoes = item.pop('cargoes', None)
    for cargo in cargoes:
        item['cargo'] = cargo
        yield item


def normalize_date(raw_date, reported_date, event):
    """Normalize raw date to an ISO8601 compatible timestamp.

    Raw dates can come in two formats:
        - 1st format: DD/MM, where DD is day of month and MM is the month
        - 2nd format: DDhhmm, where DD is day of month and hhmm is the time in 24hr format

    Some examples:
        - 1st format: '01/07'
        - 2nd format: '010830'

    Raw dates as given by the source do not provide month and/or year, so this info needs to be
    inferred from the reported date, together with the associated vessel movement type.

    A naive approach using month at scraping time may lead to inaccurate dates, especially
    during the rollover period for each month/year where dates from the previous and current
    month can appear together in the same table.

    To solve this, we compare if date is in the past or future:
        - For "eta" and "etb" vessel movements, reject if computed date is in the future.
          Then, return computed date adjusted 1 month/year prior.
        - For other vessel movements, reject if computed date is in the past.
          Then, return the computed date adjusted 1 month/year into the future.

    TODO use time information in raw date string

    Examples:
        >>> normalize_date('032200', dt.date(year=2018, month=6, day=29), 'eta')
        '2018-07-03T00:00:00'
        >>> normalize_date('032200', dt.date(year=2018, month=6, day=29), 'berthed')
        '2018-06-03T00:00:00'
        >>> normalize_date('032200', dt.date(year=2018, month=6, day=1), 'etb')
        '2018-06-03T00:00:00'
        >>> normalize_date('032200', dt.date(year=2018, month=6, day=1), 'berthed')
        '2018-05-03T00:00:00'
        >>> normalize_date('032200', dt.date(year=2018, month=12, day=30), 'eta')
        '2019-01-03T00:00:00'
        >>> normalize_date('302200', dt.date(year=2019, month=1, day=2), 'berthed')
        '2018-12-30T00:00:00'
        >>> normalize_date('02/07', dt.date(year=2018, month=7, day=5), 'eta')
        '2018-07-02T00:00:00'
        >>> normalize_date('02/01', dt.date(year=2018, month=12, day=30), 'eta')
        '2019-01-02T00:00:00'
        >>> normalize_date('30/12', dt.date(year=2019, month=1, day=2), 'berthed')
        '2018-12-30T00:00:00'

    Args:
        raw_date (str): raw date string
        reported_date (dt.datetime): used to infer missing month/year in the dates
        event (str): used to determine if we should increment month/year when inferring

    Returns:
        str: ISO8601 formatted timestamp

    """
    # normalize 1st format
    if '/' in raw_date:
        day, month, year = raw_date[:2], raw_date[3:], reported_date.year

    # normalize 2nd format
    else:
        day, month, year = raw_date[:2], reported_date.month, reported_date.year
        if event in ('eta', 'etb'):
            if reported_date.day > int(day):
                month = (reported_date + relativedelta(months=1)).month
        else:
            if reported_date.day < int(day):
                month = (reported_date - relativedelta(months=1)).month

    # handle year rollover scenarios
    if int(month) == 12 and reported_date.month == 1:
        year -= 1
    elif int(month) == 1 and reported_date.month == 12:
        year += 1

    return may_apply(f'{day}/{month}/{year}', to_isoformat)


def normalize_cargo(raw_cargo):
    """Normalize cargo string into 1 or more products and corresponding quantities

    Examples:  # noqa
        >>> list(normalize_cargo('GO/145000'))
        [{'product': 'GO', 'volume': '145000', 'movement': 'load', 'volume_unit': 'tons'}]
        >>> list(normalize_cargo('GO/1500+FO/28800')) # doctest: +NORMALIZE_WHITESPACE
        [{'product': 'GO', 'volume': '1500', 'movement': 'load', 'volume_unit': 'tons'}, {'product': 'FO', 'volume': '28800', 'movement': 'load', 'volume_unit': 'tons'}]
        >>> list(normalize_cargo('GO/JETA1/145000'))
        [{'product': 'GO/JETA1/145000', 'volume': None, 'movement': 'load', 'volume_unit': 'tons'}]

    Args:
        raw_cargo (str): raw cargo data

    Yields:
        Dict[str, str]:

    """
    for cargo in [may_strip(product) for product in raw_cargo.split('+')]:
        if len(cargo.split('/')) == 2:
            product, quantity = cargo.split('/')
        else:
            product, quantity = cargo, None

        yield {
            'product': may_strip(product),
            'volume': may_strip(quantity) if quantity else None,
            # report contains loading ops only
            'movement': 'load',
            'volume_unit': Unit.tons,
        }


def field_mapping(**kwargs):
    return {
        'Arrived': ('arrival', lambda x: normalize_date(x, **kwargs, event='arrived')),
        'Berth No': ('berth', None),
        'Berthed': ('berthed', lambda x: normalize_date(x, **kwargs, event='berthed')),
        'Clrd/Nomd': (ignore_key('irrelevant')),
        'Comm ldng': (ignore_key('irrelevant')),
        'Compltd ldng': (ignore_key('irrelevant')),
        'Dest': (ignore_key('irrelevant')),
        'event': ('event', None),
        'Ggo/Qtty': ('cargoes', lambda x: list(normalize_cargo(x))),
        'L/C': (ignore_key('irrelevant')),
        'Eta': ('eta', lambda x: normalize_date(x, **kwargs, event='eta')),
        'Etb': ('etb', lambda x: normalize_date(x, **kwargs, event='etb')),
        'Etc': (ignore_key('irrelevant')),
        'Ets': (ignore_key('irrelevant')),
        'Pier': (ignore_key('irrelevant')),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'Sailed': (ignore_key('irrelevant')),
        'Shippers': (ignore_key('irrelevant')),
        'Vessel': ('vessel', lambda x: {'name': x}),
    }
