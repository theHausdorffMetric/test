import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


UNIT_MAPPING = {'MT': Unit.tons}


BLACKLIST = ['UNKNOWN PARTY', 'UNKNOWN']


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'load',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': item.pop('cargo_units', None),
    }

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('laycan'), item['reported_date']
    )

    return item


def field_mapping():
    return {
        'vessel': ('vessel', lambda x: {'name': x}),
        'charterer': ('charterer', lambda x: normalize_charter_and_rate(x)),
        'volume': ('cargo_volume', lambda x: x.replace(',', '')),
        'units': ('cargo_units', lambda x: UNIT_MAPPING.get(x, x)),
        'cargo': ('cargo_product', None),
        'departure_zone': ('departure_zone', None),
        'laycan': ('laycan', None),
        'rate': ('rate_value', lambda x: normalize_charter_and_rate(x)),
        'arrival_zone': ('arrival_zone', lambda x: [x]),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_charter_and_rate(raw_string):
    """Normalize charter and rate, remove unwanted strings.

    Examples:
        >>> normalize_charter_and_rate('GSCALTEX')
        'GSCALTEX'
        >>> normalize_charter_and_rate('UNKNOWN')

    Args:
        raw_vessel (str):

    Returns:
        str:

    """
    return None if raw_string in BLACKLIST else raw_string


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    In this report, the lay can date can vary differently, however, we only extract below formats:
    - 07 DECEMBER format 1
    - 09-10 AUGUST format 2
    - 30JULY-01AUGUST format 3



    No month rollover found yet, don't handle them first as we don't want to make false assumptions.

    Examples:
        >>> normalize_lay_can('07 DECEMBER', '22 Dec 2018')
        ('2018-12-07T00:00:00', '2018-12-07T00:00:00')
        >>> normalize_lay_can('09-10 DECEMBER', '22 Dec 2018')
        ('2018-12-09T00:00:00', '2018-12-10T00:00:00')
        >>> normalize_lay_can('30JULY-01AUGUST', '22 Dec 2018')
        ('2018-07-30T00:00:00', '2018-08-01T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]:

    """
    rpt_year = parse_date(reported).year
    rpt_month = parse_date(reported).month

    _match_single_date = re.match(r'^([0-9]+)\s([A-z]+)$', raw_lay_can)
    if _match_single_date:
        start_day, month = _match_single_date.groups()

        # year rollover
        if month == 'DECEMBER' and 'Jan' in reported:
            rpt_year -= 1
        if (month == 'JANUARY') and 'Dec' in reported:
            rpt_year += 1

        start = to_isoformat(f'{start_day} {month} {rpt_year}', dayfirst=True)

        # return the same start date for lay_can_end is single date provided
        return start, start

    _match_double_dates = re.match(r'^([0-9]+)\-([0-9]+)\s([A-z]+)$', raw_lay_can)
    if _match_double_dates:
        start_day, end_day, month = _match_double_dates.groups()
        month = parse_date(month).month

        # year rollover
        if (month == 'DECEMBER') and ('Jan' in reported):
            rpt_year -= 1
        if (month == 'JANUARY') and ('Dec' in reported):
            rpt_year += 1

        # handle edge cases, for Jan 2019 report, some november 2018 fixtures were still
        # inside. This resulted in the insertion of november 2019 information
        if abs(rpt_month - month) > 5:
            rpt_year -= 1

        # to accomodate end dates for february
        try:
            start = to_isoformat(f'{start_day} {month} {rpt_year}', dayfirst=True)
        except:  # FIXME  # noqa
            start = to_isoformat(f'{start_day} {month - 1} {rpt_year}', dayfirst=True)

        # some end dates do not exist for this file i.e 29-31 NOVEMBER 2018
        try:
            end = to_isoformat(f'{end_day} {month} {rpt_year}', dayfirst=True)
        except:  # FIXME  # noqa
            end = start

        return start, end

    _match_rollover = re.match(r'^([0-9]+)([A-z]+)\-([0-9]+)([A-z]+)$', raw_lay_can)
    if _match_rollover:
        start_day, start_month, end_day, end_month = _match_rollover.groups()

        # year rollover
        # 31 dec - 01 jan, reported in dec
        if (start_month == 'DECEMBER') and 'Dec' in reported:
            start_year = rpt_year
            end_year = rpt_year + 1
        elif (start_month == 'JANUARY') and 'Jan' in reported:
            start_year = rpt_year - 1
            end_year = rpt_year
        else:
            start_year = end_year = rpt_year

        start = to_isoformat(f'{start_day} {start_month} {start_year}', dayfirst=True)
        end = to_isoformat(f'{end_day} {end_month} {end_year}', dayfirst=True)

        return start, end

    return None, None
