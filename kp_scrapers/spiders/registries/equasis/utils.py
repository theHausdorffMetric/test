import calendar
import random
import string

from six.moves import range


def to_timestamp(dt):
    """Converts a datetime object to a UTC timestamp.
    Naive datetime will be considered as being in UTC.
    """
    return calendar.timegm(dt.utctimetuple())


def _random_string(length=12, universe=string.ascii_lowercase):
    return ''.join(random.choice(universe) for _ in range(length))


def session_id():
    # encapsulate generation in case we want to use uuid or whatever in the future
    return _random_string()
