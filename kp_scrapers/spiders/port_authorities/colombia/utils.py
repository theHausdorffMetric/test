import datetime as dt
import hashlib


# number of retries permitted before cancelling spider job
RETRY_TIMES = 20
# date format used in website
DEFAULT_DATE_FORMAT = '%d/%m/%Y'


def build_date_string(fmt=DEFAULT_DATE_FORMAT, offset=0):
    """Get datetime string offset by N days from current time.

    Args:
        fmt (str): string format of date to be serialised
        offset (int | str): numeric of the amount of offset, in days

    Returns:
        str:
    """
    return dt.datetime.strftime(dt.datetime.utcnow() + dt.timedelta(days=offset), fmt)


def get_md5(string):
    """Convert a string into an MD5 hash.

    Args:
        string (str):

    Returns:
        str:
    """
    return hashlib.md5(string.encode('utf-8')).hexdigest()
