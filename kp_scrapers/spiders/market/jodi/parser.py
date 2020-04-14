import calendar
import datetime as dt
import re

from kp_scrapers.models.units import Unit


REPORT_TYPE = {'0': 'primary', '1': 'secondary'}

# TODO use Unit enum
UNIT = {
    '0': 'kb/d',  # Thousand Barrels per day (kb/d)
    '1': Unit.kilobarrel,  # Thousand Barrels (kbbl)
    '2': Unit.kiloliter,  # Thousand Kilolitres (kl)
    '3': Unit.kilotons,  # Thousand Metric Tons (kmt)
    '4': Unit.kilotons,  # Conversion factor barrels/ktons
}

_PRIMARY_PRODUCT = {'0': 'Crude oil', '1': 'NGL', '2': 'Other', '4': 'Total'}

_SECONDARY_PRODUCT = {
    '0': 'LPG',
    '1': 'Naphtha',
    '2': 'Gasoline',
    '3': 'Kerosene/Jet',
    '4': 'Jet',
    '5': 'Gasoil/Diesel',
    '6': 'Fuel oil',
    '7': 'Other oil products',
    '8': 'Total oil products',
}

PRODUCT = {'0': _PRIMARY_PRODUCT, '1': _SECONDARY_PRODUCT}


_PRIMARY_BALANCE = {
    '0': 'Production',
    '1': 'From other sources',
    '2': 'Import',
    '3': 'Export',
    '4': 'Products transferred/Backflows',
    '5': 'Direct use',
    '6': 'Stock change',
    '7': 'Statistical difference',
    '8': 'Refinery intake',
    '9': 'Closing stocks',
}

_SECONDARY_BALANCE = {
    '0': 'Refinery output',
    '1': 'Receipts',
    '2': 'Import',
    '3': 'Export',
    '4': 'Products transferred',
    '5': 'Interproduct transfers',
    '6': 'Stock change',
    '7': 'Statistical difference',
    '8': 'Demand',
    '9': 'Closing stocks',
}

BALANCE = {'0': _PRIMARY_BALANCE, '1': _SECONDARY_BALANCE}

ZONE_MAPPING = {
    'Chinese Taipei': 'Taiwan',
    'define': None,  # filler country name
    'Egypt (Arab Rep.)': 'Egypt',
    'Hong Kong China': 'Hong Kong',
    'Iran (Islamic Rep.)': 'Iran',
    'Korea': 'South Korea',
    'Syria (Arab Rep.)': 'Syria',
    'The former Yugoslav Rep. of Macedonia': 'North Macedonia',
    'Trinidad/Tobago': 'Trinidad and Tobago',
    'United States of America': 'United States',
}

# Inorder for the country to be identified correctly, we need to pass the country type also
# UAE is classified as country_checkpoint in our platform, hence this mapping
COUNTRY_TYPE_MAPPING = {'United Arab Emirates': 'country_checkpoint'}


def get_period(raw_period):
    """Get a DateTimeRange given a raw period string.

    Examples:
        >>> get_period('Jun2019')
        ('2019-06-01T00:00:00', '2019-07-01T00:00:00')
        >>> get_period('Dec2019')
        ('2019-12-01T00:00:00', '2020-01-01T00:00:00')

    """
    # assumption is that periods are formatted as `MonYYYY`
    matches = re.match(r'(?P<month>[a-zA-Z]{3})(?P<year>\d{4}$)', raw_period)
    year, month = matches['year'], _month_index(matches['month'])

    # get lower bound
    lower_bound = dt.datetime(year=int(year), month=month, day=1)

    # get upper bound (also account for year rollover if month is december)
    year = str(int(year) + 1) if month == 12 else year
    month = 1 if month == 12 else month + 1
    upper_bound = dt.datetime(year=int(year), month=month, day=1)

    # default: lower_bound is inclusive, upper_bound is exclusive
    return lower_bound.isoformat(), upper_bound.isoformat()


def is_valid_period(candidate):
    """Check if the period string is in correct format: MonYYYY.

    Args:
        candidate:

    Returns:

    """
    _match = re.match(r'^[a-zA-Z]{3}\d{4}$', candidate)
    if _match:
        return True

    return False


def _month_index(month):
    """Return the month number
    Args:
        str
    Return:
        int
    """
    try:
        return list(calendar.month_abbr).index(month.title())
    except ValueError:
        return None
