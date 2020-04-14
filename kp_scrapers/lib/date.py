# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import calendar
import datetime as dt
from logging import getLogger
import re
import time

import dateutil.parser
from dateutil.relativedelta import relativedelta
import pytz
import six

from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import protect_against
from kp_scrapers.settings import MONTH_LOOK_BACK_CUSTOMS_SPIDERS


logger = getLogger(__name__)

_MONTH_DAY_HOUR_MINUTE_DATE_FORMAT = re.compile(
    '(?P<month>\d+)-(?P<day>\d+)' ' (?P<hour>\d+):(?P<minute>\d+)'
)
_YEARS_MONTHS_DAYS_MINUTES_HOURS_SECONDS_AGO_FORMAT = re.compile(
    '((?P<years>\d+)(:?y))?((?P<months>\d+)(:?m))?'
    '((?P<days>\d+)(:?d))?((?P<hours>\d+)(:?h))?'
    '((?P<minutes>\d+)(:?m))?((?P<seconds>\d+)(:?s))?(:? ?ago.*)'
)

# The "Origin of Times" (as per UNIX definition) in the UTC timezone.
EPOCH_WITH_UTC_TZ = dt.datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.utc)

# NOTE can we use iso 8601 instead ?
ISODATE_WITH_SPACE = '%Y-%m-%d %H:%M:%S'
ISO8601_FORMAT = '%Y-%m-%dT%H:%M:%S'
VAGUE_DAY_MAPPING = {'ELY': ('1', '7'), 'MID': ('14', '21'), 'END': ('25', '30')}


@protect_against((ValueError))
def may_parse_date_str(date_str, fmt=ISODATE_WITH_SPACE):
    """Create datetime object from string

    Args:
        date_str (str): date string to transform
        fmt (str, optional): input date format

    Returns:
        datetime: datetime object

    Examples:
        >>> may_parse_date_str('11/28/2017', '%m/%d/%Y')
        datetime.datetime(2017, 11, 28, 0, 0)
    """
    return dt.datetime.strptime(date_str, fmt)


def create_str_from_time(date_val, fmt=ISODATE_WITH_SPACE):
    """Create str from datetime object

    Args:
        date_val (datetime.datetime): datetime object to transform
        fmt (str, optional): output date format

    Returns:
        str: date string

    Examples:
        >>> create_str_from_time(dt.datetime(2017, 11, 28, 2, 30))
        '2017-11-28 02:30:00'
    """
    return date_val.strftime(fmt)


def rewind_time(current_date, months):
    """List of past dates.

    Args:
        current_date(datetime.datetime): starting date to considere
        months(int): number of months to look back before current date

    Returns:
        List[Date]

    """
    dates = [current_date]
    while months > 0:
        new_date = current_date + relativedelta(months=-1)
        dates.append(new_date)
        current_date = new_date
        months -= 1

    return dates


def get_month_look_back(months_look_back, start_date):
    def diff_month(d1, d2):
        return abs((d1.year - d2.year) * 12 + d1.month - d2.month)

    look_back = MONTH_LOOK_BACK_CUSTOMS_SPIDERS
    if months_look_back:
        look_back = int(months_look_back)
    elif start_date:
        start_date = dateutil.parser.parse(start_date, dayfirst=False, yearfirst=True)
        look_back = diff_month(start_date, dt.datetime.now())

    return look_back


def str_month_day_time_to_datetime(
    datestring, max_ahead=dt.timedelta(days=60), max_lag=dt.timedelta(seconds=0)
):
    """Turns date denoted by 'mm-dd HH:MM' into actual dates.

    Args:
        max_ahead(datetime.timedelta): if the given date is further from now
        than this delta, decrement a year.
        max_lag(datetime.timedelta): if the given date is further in the past
        than this delta, discard the value

    Warning:

       At the turn of a year this function will assume that functions received
       late for the previous year will be turned into dates for the end of the
       new year !
    """
    match = _MONTH_DAY_HOUR_MINUTE_DATE_FORMAT.match(datestring)
    if match is not None:
        parsed_date = {k: int(v) for k, v in six.iteritems(match.groupdict())}

        month = int(parsed_date['month'])
        day = int(parsed_date['day'])
        if day == 0 and month == 0:
            return None  # All zeros date used to denote invalid dates.

        now = dt.datetime.now()
        try:
            the_date = dt.datetime(year=now.year, **parsed_date)
        except ValueError:
            logger.warning('Invalid input: {}'.format(datestring))
            return None

        earliness = the_date - now
        if earliness > max_ahead:
            the_date = the_date.replace(year=the_date.year - 1)

        tardiness = now - the_date
        if tardiness > max_lag:
            return None

        return the_date.isoformat()

    return None


def get_last_day_of_previous_month(input_date):
    """Get last day of the month before date

    Args:
        date (Union[datetime.date, datetime.datetime]): Description

    Returns:
        Union[datetime.date, datetime.datetime]: last day of previous month

    Exemples:
        >>> get_last_day_of_previous_month(dt.datetime(2012, 1, 1, 1, 1, 1))
        datetime.datetime(2011, 12, 31, 1, 1, 1)
        >>> get_last_day_of_previous_month(dt.date(2011, 1, 1))
        datetime.date(2010, 12, 31)

    """
    return input_date.replace(day=1) - dt.timedelta(days=1)


def get_first_day_of_next_month(input_date):
    """Get first day of the month after date

    Args:
        date (Union[datetime.date, datetime.datetime]): Description

    Returns:
        Union[datetime.date, datetime.datetime]: first day of next month

    Exemples:
        >>> get_first_day_of_next_month(dt.datetime(2012, 12, 12, 12, 12, 12))
        datetime.datetime(2013, 1, 1, 12, 12, 12)
        >>> get_first_day_of_next_month(dt.date(2012, 12, 12))
        datetime.date(2013, 1, 1)

    """
    return (input_date.replace(day=1) + dt.timedelta(days=32)).replace(day=1)


def get_last_day_of_current_month(month_str, year_str, month_fmt, year_fmt='%Y'):
    """Get last day of current month.

    Examples:
        >>> get_last_day_of_current_month('02', '2000', '%m', '%Y')
        29
        >>> get_last_day_of_current_month('Feb', '2019', '%b', '%Y')
        28
        >>> get_last_day_of_current_month('9', 2018, '%m', '%Y')
        30

    Args:
        month_str (str):
        year_str (str | int):
        month_fmt (str): %b, %B, %m
        year_fmt (str): %y, %Y

    Returns:
        int: last day of current month

    """
    month = dt.datetime.strptime(month_str, month_fmt).month
    year_str = str(year_str)
    year = dt.datetime.strptime(year_str, year_fmt).year

    return calendar.monthrange(year, month)[1]


def to_isoformat(date_str, dayfirst=True, yearfirst=False, tz=None, **kwargs):
    """Convert a raw date string into an ISO-8601 compatible date string.

    Args:
        date_str (str): date string in a recognisable fuzzy format
        dayfirst (bool): does day come before month in `date_str`?
        yearfirst (bool): does year come before month/day in `date_str`?
        tz (str): force timezone information
        **kwargs:

    Returns:
        str: date string in ISO-8601 format

    Examples:
        >>> to_isoformat('2015-06-12 10:46:46 GMT', dayfirst=False)
        '2015-06-12T10:46:46'
        >>> to_isoformat('2015-06-12 10:46:46 GMT')
        '2015-12-06T10:46:46'
        >>> to_isoformat('12-06-2015 10:46:46 +02:00', dayfirst=False)
        '2015-12-06T10:46:46'
        >>> to_isoformat('12-06-2015 10:46:46 +02:00')
        '2015-06-12T10:46:46'
        >>> to_isoformat('12-06-2015 10:46:46 GMT', tz='UTC')
        '2015-06-12T10:46:46+00:00'
        >>> to_isoformat('')
        >>> to_isoformat(None)

    """
    if not date_str:
        return None

    return (
        dateutil.parser.parse(date_str, dayfirst=dayfirst, yearfirst=yearfirst, **kwargs)
        .replace(tzinfo=pytz.timezone(tz) if tz else None)
        .isoformat()
    )


def is_isoformat(date_str):
    """Check is string is formatted as a valid ISO-8601 extended timestamp.

    Examples:
        >>> is_isoformat('2018-06-03')
        True
        >>> is_isoformat('2018-06-03T21:02:06')
        True
        >>> is_isoformat('2018-06-03T21:02:06Z')
        True
        >>> is_isoformat('2018-06-03T21:02:06+00:00')
        True
        >>> is_isoformat('2018-06-03T21:02:06.593194+00:00')
        True

        # wrong format dd-mm-yyy
        >>> is_isoformat('03-06-2018')
        False

        # space as a delimiter instead of "T"
        >>> is_isoformat('2018-06-03 21:02:06')
        False

        # forward-slash delimiters
        >>> is_isoformat('2018/06/03T21:02:06')
        False

        # invalid month/day/time figures
        >>> is_isoformat('2018-56-93 71:99:99')
        False

    """
    return (
        re.match(
            (
                r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])'  # yyyy-mm-dd  # noqa
                '(T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?'  # hh:mm:ss.fff
                '(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?)?$'  # timezone
            ),
            date_str,
        )
        != None
    )  # noqa


def daterange(start_date, end_date):
    """Get dates between two dates (included)

    Args:
        start_date (Union[datetime.date, datetime.datetime]):
        end_date (Union[datetime.date, datetime.datetime]):

    Yields:
        Union[datetime.date, datetime.datetime]:

    Examples:
        >>> list(daterange(dt.date(2018, 1, 1), dt.date(2018, 1, 3)))
        [datetime.date(2018, 1, 1), datetime.date(2018, 1, 2), datetime.date(2018, 1, 3)]
        >>> list(daterange(dt.datetime(2018, 1, 1), dt.datetime(2018, 1, 3)))
        [datetime.datetime(2018, 1, 1, 0, 0), datetime.datetime(2018, 1, 2, 0, 0), \
datetime.datetime(2018, 1, 3, 0, 0)]
        >>> list(daterange(dt.datetime(2018, 1, 1), dt.datetime(2018, 1, 1)))
        [datetime.datetime(2018, 1, 1, 0, 0)]
    """
    for delta in six.moves.range(int((end_date - start_date).days) + 1):
        yield start_date + dt.timedelta(delta)


def system_tz_offset():
    """Get the timezone offset of the local machine.

    Takes daylight savings into consideration.

    Returns:
        float: timezone offset (in hours)

    """
    offset = time.timezone if not time.localtime().tm_isdst else time.altzone
    # there are 3600 seconds in an hour
    # NOTE `time.timezone/altzone` takes timezones ahead of UTC zone as negative,
    # which is the reverse of known convention
    return offset / -3600


def convert_aspnet_timestamp(timestamp):
    """Converts POSIX timestamp to ISO formatted string in UTC

    When extracting data via loading the json response from asp.net backend the `datetime` will
    be in a POSIX timestamp from epoch in milliseconds such as '/Date(1530180000000+1000)/'.
    The timestamp '1530181000' is the clean timestamp extracted.
    This converts the timestamp to an ISO-formatted string in UTC

    Args:
        timestamp(str): fuzzy posix timestamp (in milliseconds)

    Returns:
        datetime(str): ISO format datetime

    Examples:
        >>> convert_aspnet_timestamp('/Date(1530180000000+1000)/')
        '2018-06-28T10:16:40'
    """
    ts, tz = timestamp.split('(')[1].split(')')[0].split('+')
    # convert milliseconds to seconds
    ts = int(int(ts) / 1000)
    return dt.datetime.utcfromtimestamp(ts + int(tz)).isoformat()


def get_date_range(date_range_str, month_seperator, day_seperator, reported_date):
    """Normalize raw laycan string. This assumes that the month in rollover
    dates references the forward month

    FIXME: This function is still very specific for spot charters. To be placed
    somewhere else

    Raw date inputs can be of the following formats:
        - range: '15-17/6'
        - range with month rollover: '31-1/6'
        - single day: '14/3'
        - english month: '14/SEP'
        - vague date: 'MID/SEP'
        - vague month: 'OCT-DEC'

    Args:
        start_end_str (str):
        reported_date (str):

    Returns:
        Tuple[str]: tuple of date range period

    Examples:
        >>> get_date_range('26-28/3', '/', '-','25 Mar 2018')
        ('2018-03-26T00:00:00', '2018-03-28T00:00:00')
        >>> get_date_range('31-3/6', '/', '-','25 Jun 2018')
        ('2018-05-31T00:00:00', '2018-06-03T00:00:00')
        >>> get_date_range('1-2/1', '/', '-','25 Dec 2018')
        ('2019-01-01T00:00:00', '2019-01-02T00:00:00')
        >>> get_date_range('29-30/12', '/', '-','01 Jan 2019')
        ('2018-12-29T00:00:00', '2018-12-30T00:00:00')
        >>> get_date_range('30-2/1', '/', '-','25 Jan 2018')
        ('2017-12-30T00:00:00', '2018-01-02T00:00:00')
        >>> get_date_range('30-2/1', '/', '-','25 Dec 2017')
        ('2017-12-30T00:00:00', '2018-01-02T00:00:00')
        >>> get_date_range('12/3', '/', '-','25 Mar 2018')
        ('2018-03-12T00:00:00', '2018-03-12T00:00:00')
        >>> get_date_range('14/SEP', '/', '-','25 Sep 2018')
        ('2018-09-14T00:00:00', '2018-09-14T00:00:00')
        >>> get_date_range('MID/SEP', '/', '-','25 Sep 2018')
        ('2018-09-14T00:00:00', '2018-09-21T00:00:00')
    """
    # normalize lay_can to obtain consistency
    date_range_str = date_range_str.replace(month_seperator, '/').replace(day_seperator, '-')
    # extract the month and date range first
    date_range, _, month = date_range_str.partition('/')
    if not month:
        return None, None

    # get reference year
    year = dateutil.parser.parse(reported_date).year
    # standardise month to numeric else leave as str (could be require vague mapping)
    month = dateutil.parser.parse(month).month if not try_apply(month, int) else month.lower()

    # get start and end day
    if len(date_range.split(day_seperator)) == 1:
        # take care of scenario where we have vague day descriptors (i.e., MID/END)
        if try_apply(date_range, int):
            start = end = date_range
        else:
            start, end = VAGUE_DAY_MAPPING.get(date_range, ('1', '30'))
    elif len(date_range.split(day_seperator)) == 2:
        start, end = date_range.split('-')
    else:
        raise ValueError(f'Unknown raw date range format: {date_range_str}')

    # Year shift Dec case
    if int(month) == 12 and int(dateutil.parser.parse(reported_date).month) == 1:
        year -= 1
    # Year shift Jan case
    if int(month) == 1 and int(dateutil.parser.parse(reported_date).month) == 12:
        year += 1

    # init laycan period
    # sometimes, we may be presented with dates like `31-3/6`, which contains a month rollover
    # hence, we need to use try/except
    try:
        _end = dateutil.parser.parse(f'{end} {month} {year}', dayfirst=True)
    except ValueError:
        return None
    try:
        _start = dateutil.parser.parse(f'{start} {month} {year}', dayfirst=True)
    except ValueError:
        _start = dateutil.parser.parse(f'{start} {int(month) - 1} {year}', dayfirst=True)
    if _start > _end:
        _start -= relativedelta(months=1)

    return _start.isoformat(), _end.isoformat()
