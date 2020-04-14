"""Module for translating UN/LOCODEs into proper location names.

Usage
~~~~~

    .. code-block:: Python

        # valid code
        >>> get_location('PECLL')
        'Callao, PE'

        # invalid code
        >>> get_location('foobar')

"""
import logging
import re

from requests import Session


logger = logging.getLogger(__name__)


__ENDPOINT = 'http://locode.info/{unlocode}'
__LOCATION_CACHE = {}
__LOCATION_PATTERN = r'<h1>\w+:\s*(.*)<\/h1>'
__SESSION = None


def get_location(unlocode):
    """Get human name of location from raw UN/LOCODE.

    Args:
        unlocode (str):

    Returns:
        str | None: location string returned if found, else None

    Raises:
        ValueError: only if structure of endpoint has changed
    """
    global __LOCATION_CACHE

    # retrieve cached result, if it exists
    if __LOCATION_CACHE.get(unlocode):
        logger.debug(f'Retrieving location from cache: {unlocode}')
        return __LOCATION_CACHE[unlocode]

    res = _get_session().get(__ENDPOINT.format(unlocode=unlocode))

    # unlocode does not exist, return None
    if not res.ok:
        logger.warning(f'Unknown UN/LOCODE: {unlocode}')
        return None

    # match unlocode and extract location name
    _match = re.search(__LOCATION_PATTERN, res.text)
    if not _match:
        raise ValueError(f'Unable to retrieve location, resource has likely changed')

    # cache result for faster retrieval next time
    __LOCATION_CACHE[unlocode] = _match.group(1)
    return _match.group(1)


def _get_session(recreate=False):
    """Obtain a session for making multiple requests to an endpoint.

    This allows reuse of the TCP connection, improving performance for multiple calls
    to the endpoint.

    Args:
        recreate (bool): init new session if True

    Returns:
        requests.Session:
    """
    global __SESSION

    if not __SESSION or recreate:
        __SESSION = Session()

    return __SESSION
