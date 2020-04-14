import calendar
from calendar import month_abbr, month_name
import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

CHARTER_STATUS_MAPPING = {
    'FXD': 'Fully Fixed',
    'FLD': 'Failed',
    'RPLCA': 'Fully Fixed',
    'RPTD': 'Fully Fixed',
    'FXD_RPLC': 'Fully Fixed',
    'SUBS': 'On Subs',
    'RPLC': 'Fully Fixed',
}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        Dict[str, str]:

    Returns:
        Dict[str, Any]:

    """
    raw_item = normalize_rawitem(raw_item)
    if not raw_item:
        return

    item = map_keys(raw_item, field_mapping())

    # build and clean vessel
    vessel_name = item.pop('vessel', '')
    item['vessel'] = {'name': re.sub(r'\(.*', '', vessel_name)}

    # fetch charter_status from vessel name
    _match = re.search(r'(?:\()([\*-]*)?(?P<status>[A-Z]*)(?:\))?', vessel_name)
    if _match and not item.get('status'):
        item['status'] = CHARTER_STATUS_MAPPING.get(_match.group('status'), None)

    # build cargo sub model
    item['cargo'] = {
        'product': item.pop('product', ''),
        'volume': re.sub(r'\D', '', item.pop('volume', '')),
        'volume_unit': Unit.kilotons,
        'movement': 'load',
    }

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can', ''), item['reported_date']
    )

    return item


def field_mapping():
    return {
        'Vessel_name': ('vessel', None),
        'volume': ('volume', None),
        'product': ('product', None),
        'departure_zone': ('departure_zone', None),
        'arrival_zone': ('arrival_zone', None),
        'lay_can': ('lay_can', None),
        'charterer': ('charterer', None),
        'charterer_status': ('status', lambda x: CHARTER_STATUS_MAPPING.get(x)),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    Lay can date pattern:
        - Normal pattern: 03/12, 10-12/12
        - Rollover pattern: 30-01/12

    Examples:
        >>> normalize_lay_can('03/12', '26 Nov 2018')
        ('2018-12-03T00:00:00', '2018-12-03T00:00:00')
        >>> normalize_lay_can('10-12/12', '26 Nov 2018')
        ('2018-12-10T00:00:00', '2018-12-12T00:00:00')
        >>> normalize_lay_can('30-01/12', '26 Nov 2018')
        ('2018-11-30T00:00:00', '2018-12-01T00:00:00')
        >>> normalize_lay_can('28-01/01', '27 Dec 2018')
        ('2018-12-28T00:00:00', '2019-01-01T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]:

    """
    _match = re.search(r'(\d{1,2})-?(\d{1,2})?/(\d{1,2})', raw_lay_can)
    if _match:
        start_day, end_day, month = _match.groups()
        year = _get_lay_can_year(month, reported)
        # to handle roll over case before converting them to date type
        start_month = (
            _get_previous_month(month) if end_day and int(start_day) > int(end_day) else month
        )
        # this block is to safeguard from handling dates like '29/02'
        try:
            lay_can_start = _build_lay_can_date(start_day, start_month, year)
            lay_can_end = _build_lay_can_date(end_day, month, year) if end_day else lay_can_start
        except (ValueError):
            logger.warning(f'Invalid date entry in the report: {raw_lay_can}')
            return None, None

        # if it's rollover case, this is to rewind the year if the dates are between two years
        if lay_can_start > lay_can_end:
            lay_can_start = lay_can_start + relativedelta(years=-1)

        return lay_can_start.isoformat(), lay_can_end.isoformat()

    logger.error(f'Invalid or new lay can date pattern: {raw_lay_can}')
    return None, None


def _get_lay_can_year(month, reported):
    """Get lay can year.

    Args:
        month (str):
        reported (str):

    Returns:
        int:

    """
    year = parse_date(reported).year
    if ('12' == month or '11' == month) and 'Jan' in reported:
        year -= 1
    if ('01' == month or '1' == month) and 'Dec' in reported:
        year += 1
    return year


def _build_lay_can_date(day, month, year):
    """Build lay can date.

    Args:
        day (int | str):
        month (str):
        year (int):

    Returns:
        datetime.datetime: time in ISO 8601 format

    """
    return parse_date(f'{day} {month} {year}', dayfirst=True)


def _get_previous_month(month):
    return int(month) - 1 if int(month) != 1 else 12


def normalize_rawitem(raw_item):
    """ normalize the raw item to standard format
    Args:
        Dict[str, str]
    Return:
        Dict[str, Any]
    """

    # parse and normalize the lay can date to a suitable format and
    # ignore the record that doesnt have months
    date_string = date_parser(raw_item.get('lay_can'))
    if date_string is None:
        return

    raw_item['lay_can'] = date_string

    normalize_arrival_zone(raw_item)

    return raw_item


def normalize_arrival_zone(raw_record):
    """Normalize the arrival zone to seperate out the multiple zones

    Args:
        Dict[str, str]
    Return:
        Dict[str, Any]
    """

    arrival_zone = raw_record.get('arrival_zone')
    if arrival_zone:
        raw_record['arrival_zone'] = arrival_zone.split('-')


def date_parser(date_string):
    """Parse the date and convert it into standard format

    Args:
        str
    Return:
        str

    Examples:
        >>> date_parser('21-22')
        >>> date_parser('23-24/11')
        '23-24/11'
        >>> date_parser('18/NOV')
        '18/11'
        >>> date_parser('18/Notavail')
        >>> date_parser('23/2-1/3')
        '23-1/3'
        >>> date_parser('23/2-1/Mar')
        '23-1/3'
        >>> date_parser('18-Feb')
        '18/2'
    """
    if not date_string:
        return

    _match = re.findall(r'(\d{1,2})-?(\d{1,2})?/([\d]{1,2}|[\w]*)|(\d{1,2})-(\D+)', date_string)

    if not _match:
        return

    # the pattern can match upto 2 times only, if there is more than 2 matches
    # then the incoming date format is completely new
    if len(_match) == 1:
        #  *_ft represents the value captured from another format('27-Feb')
        start_day, end_day, month, start_day_ft, month_ft = _match[0]
    elif len(_match) == 2:
        start_day, end_day, month, start_day_ft, month_ft, date_string = _convert_format(_match)
    else:
        logger.warning(f'Invalid or new lay can date pattern: {date_string}')
        return None

    month = month if month else month_ft

    if not month:
        return

    # this to convert the '18-Feb' format to '18/Feb' so eveything follows the same format
    if '/' not in date_string:
        highest_index = date_string.rfind('-')
        date_string = date_string[:highest_index] + '/' + date_string[highest_index + 1 :]

    month_pattern = '|'.join(month_name[1:]) + '|' + '|'.join(month_abbr[1:])

    if re.match(r'\D', month.strip()):
        if re.match(r'' + month_pattern, month, re.IGNORECASE):
            return date_string.replace(month, str(_month_index(month)))
        else:
            return

    return date_string


def _convert_format(dates):
    """ Parse the '27/2-1/3' format and convert into standard format

    Args:
        List[Tuple]
    Return:
        Tuple

    Examples:
        >>> _convert_format([(27, None, 2, 21, None), (10, None, 3, None, None)])
        (None, None, 3, None, None, '27-10/3')
        >>> _convert_format([(27, None, 2, 21, None), (10, None, None, None, None)])
        (None, None, None, None, None, None)
        >>> _convert_format([(27, None, 2, 21, None), (10, None, None, None, 'Feb')])
        (None, None, 'Feb', None, None, '27-10/Feb')
    """

    # here st_* denotes the staring date format
    # en_* represents the ending date formt,
    #  *_ft represents the value captured from another format('27-Feb')
    st_start_day, _, st_month, st_start_day_ft, st_month_ft = dates[0]
    en_start_day, _, en_month, en_start_day_ft, en_month_ft = dates[1]

    # End month should always be present
    if not en_month and not en_month_ft:
        return (None, None, None, None, None, None)

    start_day = st_start_day if st_start_day else st_start_day_ft
    end_day = en_start_day if en_start_day else en_start_day_ft
    month = en_month if en_month else en_month_ft

    if end_day:
        date_string_pattern = str(start_day) + '-' + str(end_day) + '/' + str(month)
    else:
        date_string_pattern = str(start_day) + '/' + str(month)

    return (None, None, month, None, None, date_string_pattern)


def _month_index(month):
    """Return the month number
    Args:
        str
    Return:
        int
    """
    try:
        # to identify whether month or its abbrevation passed
        if len(month) > 3:
            return list(calendar.month_name).index(month.title())
        else:
            return list(calendar.month_abbr).index(month.title())
    except ValueError:
        return None
