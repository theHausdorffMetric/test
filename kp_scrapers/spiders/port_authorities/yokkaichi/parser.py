import datetime as dt
import re

from dateutil.parser import parse as parse_date
from pytz import timezone

from kp_scrapers.lib.parser import may_strip


_API_DATE_FORMAT = '%Y/%m/%d'


def get_jst_time(fmt=None, **offset):
    """Get current JST time at Yokkaichi port, with optional offset.

    Args:
        offset: keyword arguments for `dt.timedelta`

    Returns:
        str: datetime string formatted as per `fmt` argument

    """
    jst = dt.datetime.utcnow().replace(tzinfo=timezone('Asia/Tokyo'))
    return (jst + dt.timedelta(**offset)).strftime(fmt or _API_DATE_FORMAT)


def parse_datetime_range(date_range_str):
    """Get a list of days inclusively between two comma-delimited dates (whitespace tolerant).

    Args:
        date_range_str (str):

    Returns:
        List[str]:

    Examples:
        >>> parse_datetime_range('2019/05/28 ,2019/06/01')
        ['2019/05/28', '2019/05/29', '2019/05/30', '2019/05/31', '2019/06/01']

    """
    # get lower-bound date and number of days to parse from it
    _lower, _upper = date_range_str.split(',')
    lower, upper = parse_date(_lower, dayfirst=False), parse_date(_upper, dayfirst=False)

    day_diff = (upper - lower).days + 1  # inclusive of `start_date`
    return [(lower + dt.timedelta(days=day)).strftime(_API_DATE_FORMAT) for day in range(day_diff)]


def naive_parse_html(html_string, delimiter='<br>'):
    """Naively parse, remove and split element contents by linebreak HTML tag.

    Python's builtin xml library was not utilised because of the presence of non-closing <br> tags.

    Args:
        html_string (str):

    Returns:
        List[str]:

    Examples:
        >>> naive_parse_html('<span>Voyage #<br>Signal<br>Berth #</span>')
        ['Voyage #', 'Signal', 'Berth #']
        >>> naive_parse_html('<td nowrap style="text-align:right;" valign="top">44,854.00 総ト<br><br>52,190.00 DWT\\n<br></td>')  # noqa
        ['44,854.00 総ト', '', '52,190.00 DWT', '']
        >>> naive_parse_html('<td>\\n<br>\\n[備考]<br>出(1)06:00<br>\\n</td>')
        ['', '[備考]', '出(1)06:00', '']

    """
    # sanity check, in case we obtain ill-formatted strings
    contents_match = re.match(r'^<[\w\s\-\"\=\#\:\;]+>(.*)<\/.+>$', html_string, re.S)
    if not contents_match:
        raise ValueError(f'Unexpected HTML string: {html_string}')

    # split by linebreak tags
    return [may_strip(ele) for ele in contents_match.group(1).split(delimiter)]
