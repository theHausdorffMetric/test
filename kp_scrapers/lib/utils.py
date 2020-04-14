# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function
from datetime import datetime, timedelta
from functools import wraps
from inspect import isgenerator
from io import BytesIO
from itertools import zip_longest
import json
import logging
import os
import random
import re
import time
from typing import Any
import unicodedata
from zipfile import ZipFile

from scrapy.selector import Selector
import six
from six.moves import map, range


logger = logging.getLogger(__name__)


FEET_TO_METERS = 0.3048
INCH_TO_METERS = 0.0254


# TODO support decision based on arguments
def run_once(func):
    """Garantee the decorated function to execute only once."""

    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return func(*args, **kwargs)

    wrapper.has_run = False
    return wrapper


def protect_against(errors=(Exception), fallback_value=None):
    """Return None if decorated function throws the exceptions specified.

    Args:
        errors (Tuple[Exception]): tuple of protected exceptions
        fallback_value (*): value to return in place of encountered exception

    """

    def _wrapper(func):
        def _inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except errors as e:
                logger.warning('Exception in "{}": {}'.format(func.__module__, repr(e)))
                return fallback_value

        return _inner

    return _wrapper


def is_valid_item(item, mandatory):
    return all([item.get(key) is not None for key in mandatory])


def may_load_json(file_path):
    if not os.path.exists(file_path):
        # abort early
        return []

    with open(file_path, 'r') as fd:
        return json.load(fd)


def save_json(data, file_path):
    # dump/overwrite it localy for futur cold use
    with open(file_path, 'w') as fd:
        json.dump(data, fd)


def deprecated(msg):
    """Print a warning message and run like nothing happened.

    Example:
        >>> @deprecated('use `fast_add` instead')
        ... def slow_add(a, b):
        ...     return a + b
        >>> slow_add(1, 2)
        WARNING: `slow_add` is deprecated: use `fast_add` instead
        3

    """

    def _wrap(func):
        def _inner(*args, **kwargs):
            print(('WARNING: `{}` is deprecated: {}'.format(func.__name__, msg)))
            return func(*args, **kwargs)

        return _inner

    return _wrap


def search_list(collection, key, value):
    """Search for key/value pair in the given list.

    Args:
        collection (list[dict]): arbitrary list of dict to scan
        key (str): key that should be in every items

    Returns:
        dict: the first object matching key/value

    Raises:
        KeyError: the given key was not in at least one item
        ValueError: the given k/v was not found

    Examples:
        >>> search_list([{'foo': 'bar'}], 'foo', 'bar')
        {'foo': 'bar'}
        >>> search_list([{'foo': 'bar'}], 'not_here', 'bar')
        Traceback (most recent call last):
            ...
        KeyError: 'not_here'
        >>> search_list([{'foo': 'bar'}], 'foo', 'not_here')
        Traceback (most recent call last):
            ...
        ValueError: foo=not_here not found

    """
    # NOTE we don't use `.get()` on purpose, because we don't want searching with a
    # bad key to silently fail. But it forces the collection to have this key in
    # every items, which might be to much of a constraint ?
    found = next((item for item in collection if item[key] == value), False)
    if not found:
        raise ValueError('{k}={v} not found'.format(k=key, v=value))

    return found


def ignore_key(msg):
    """Tell the map_keys function to ignore a key.

    Examples:
        >>> {'some_key_name': ignore_key('I dont care about this key')}
        {'some_key_name': (None, None)}

    """
    return None, None


def map_keys(raw_obj, key_map, skip_missing=True):
    NOT_FOUND = -1
    res = {}
    for key, value in six.iteritems(raw_obj):
        mapped_key, transform = key_map.get(key, (NOT_FOUND, None))

        if mapped_key is None:
            # explicitely asked to ignore it
            continue
        elif mapped_key == NOT_FOUND and skip_missing:
            # not found so we skip it if asked for
            continue
        elif mapped_key == NOT_FOUND and not skip_missing:
            # nothing was provided so we keep this kv as is
            mapped_key = key

        try:
            res[mapped_key] = transform(value) if transform else value
        except ValueError:
            res[mapped_key] = None

    return res


def extract_row(row):
    def f(field):
        res = field.xpath('.//text()').extract()
        d = re.sub('[\t\r ]+', ' ', res[0].strip()) if len(res) == 1 else None
        return d

    return list(map(f, row.xpath('.//*[not(*)]')))


def extract_text(node):
    s = node.xpath(".//text()").extract()
    if len(s) > 0:
        return " ".join(s)
    else:
        return None


def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks.

    Taken from `itertools` recipes
    https://docs.python.org/3/library/itertools.html#recipes

    Examples:
        >>> list(grouper([1, 3, 7, 2, 5, 6], 2))
        [(1, 3), (7, 2), (5, 6)]

    """
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def mean(numbers):
    return float(sum(numbers)) / max(len(numbers), 1)


def random_delay(average: int = 10):
    """Decorator which adds a random delay before executing the wrapped function.

    This function is meant to simulate human behaviour and real-world queues,
    which can be modelled as a poisson process with random, memoryless intervals.

    """

    def _decorator(func):
        def _generator(res):
            yield from res

        @wraps(func)
        def _wrapper(*args, **kwargs):
            delay = random.expovariate(1 / average)
            logger.debug("Random delay added: %1.2f seconds", delay)
            time.sleep(delay)

            res = func(*args, **kwargs)
            # handle decorated generators, and functions
            # TODO this pattern can be made generic and applied to all decorators
            return _generator(res) if isgenerator(res) else res

        return _wrapper

    return _decorator


def strip_non_ascii(text):
    """Remove non ascii char from a string.

    Args:
        text (str):

    Returns:
        str

    Examples:
        >>> strip_non_ascii('IVORY RAY�')
        'IVORY RAY'
        >>> strip_non_ascii('foo')
        'foo'

    """
    return ''.join(c for c in text if ord(c) < 128).strip()


def retry(tries=1, wait=0):
    """A decorator that retries a method a certain number of times and wait a
    certain number of seconds between each try.

    .. todo::

       There is also a retry_on_deadlock_decorator() function in etl.orm.utils.
       Maybe se could have a simpler more generic::

       .. code-block:: Python
          # Usage:
          @retry(tries=3, wait=10)
          def do_it():
              pass

    """

    def retry_decorator(fn):
        def retry_function(*args, **kwargs):
            for _ in range(tries - 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    logger.warning('`{}` failed: {} - retrying in {}s'.format(fn.__name__, e, wait))
                    time.sleep(wait)
            return fn(*args, **kwargs)

        return retry_function

    return retry_decorator


def feet_to_meters(str_val):
    """Convert foot to meters

    Args:
        str_val (str): value foot (format x'y")

    Returns:
        int: value in meters

    Examples:
        >>> feet_to_meters('27\\'6"')
        8.38
    """
    feet, inch = [float(e) for e in str_val.replace('"', '').split("'")]
    meters = feet * FEET_TO_METERS + inch * INCH_TO_METERS
    return round(meters, 2)


def remove_diacritics(input_str):
    """Remove diacritics from a given string.

    This function uses unicode metadata to find accents and diacritical marks.
    WARNING this may do more than just accents in non-Latin scripts.

    Args:
       input_str (str): string containing accents/dia

    Returns:
        str: string containing substituted characters

    Note:
       This function has not been tested on non-Latin languages.
       A review by native users of non-latin scripts might help make sure this
       function works for those scripts too.

    """
    nkfd_form = unicodedata.normalize('NFKD', input_str)
    return u''.join([c for c in nkfd_form if not unicodedata.combining(c)])


def response_to_dict(response, include_body=False):
    """Returns a dict based on a response from a spider."""
    return {
        'status': response.status,
        'url': response.url,
        'headers': dict(response.headers),
        'body': response.body if include_body else "[...]",
    }


def compare(a, b):
    """To replace the built in cmp() function in python 2 since it's no longer
    supported in python 3
    """
    return (a > b) - (a < b)


@protect_against((ValueError))
def scale_to_thousand(quantity):
    """Common function for converting TMT to MT
    Examples:
        >>> scale_to_thousand('04')
        4000.0
    """
    return float(quantity) * 1000


def to_unicode(str_bytes):
    """Transforms a Bytestring into a Unicode
    Args:
        str_bytes (Bytestring): data needs to be decoded
    Returns:
        str: Unicode representation of str_bytes
    """
    return str_bytes.decode('utf-8') if isinstance(str_bytes, bytes) else str_bytes


def to_bytestring(str_unicode):
    """Transforms a Unicode into a Bytestring
    Args:
        str_unicode (Unicode): data needs to be encoded
    Returns:
        str: Bytestring representation of str_unicode
    """
    return str_unicode.encode('utf-8') if isinstance(str_unicode, six.text_type) else str_unicode


def map_row_to_dict(row, header):
    """Maps a list of table values to a list of table headers by index

    Examples:
        >>> map_row_to_dict(['BAKER', 'PMS'], ['vessel', 'product'])
        {'vessel': 'BAKER', 'product': 'PMS'}

    """
    return {column: row[i] for i, column in enumerate(header)}


def is_number(check_number: Any) -> bool:
    """Checks if string is numeric

    Examples:
        >>> is_number('ioi')
        False
        >>> is_number('1')
        True
        >>> is_number('1.0')
        True
        >>> is_number(None)
        False
    """
    try:
        float(check_number)
        return True
    except (ValueError, TypeError):
        return False


class throttle(object):
    """Decorator that prevents a function from being called more than once every time period.

    To create a function that cannot be called more than once a minute:
        @throttle(minutes=1)
        def my_fun():
            pass

    """

    def __init__(self, seconds=0, minutes=0, hours=0):
        self.throttle_period = timedelta(seconds=seconds, minutes=minutes, hours=hours)
        self.time_of_last_call = datetime.min

    def __call__(self, fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            now = datetime.now()
            time_since_last_call = now - self.time_of_last_call

            if time_since_last_call > self.throttle_period:
                self.time_of_last_call = now
                return fn(*args, **kwargs)

        return wrapper


def flatten_dict(dd, separator='.', prefix=''):
    """Flatten arbitrary nested dictionaries.

    Shamelessly stolen from: https://www.quora.com/How-do-you-flatten-a-dictionary-in-Python

    WARNING: it doesn't support nested arrays. This is quite annoying since
    `cargoes` is quite common for example

    Examples:
        >>> flatten_dict({'foo':  'buzz'})
        {'foo': 'buzz'}
        >>> flatten_dict({'foo': {'bar': 'buzz'}})
        {'foo.bar': 'buzz'}
        >>> flatten_dict({'foo': {'bar': 'buzz'}}, separator='_')
        {'foo_bar': 'buzz'}

    """
    return (
        {
            prefix + separator + k if prefix else k: v
            for kk, vv in dd.items()
            for k, v in flatten_dict(vv, separator, kk).items()
        }
        if isinstance(dd, dict)
        else {prefix: dd}
    )


def lookup_by(key, value, data):
    """Search an array using the given key.

    Examples:
        >>> lookup_by('imo', '123', [{'imo': '123'}])
        {'imo': '123'}
        >>> lookup_by('mmsi', '123', [{'imo': '123'}])
        >>> lookup_by('imo', '123', [{'imo': '456'}])

    """
    return next((item for item in data if item.get(key) == value), None)


def unpack_kml(func):
    @wraps(func)
    def _inner(klass, response):
        z = ZipFile(BytesIO(response.body))

        doc = next((name for name in z.namelist() if '.kml' in name), None)
        if doc is None:
            raise ValueError('unable to find the requested resource')

        kml = z.read(doc)
        return func(klass, doc, Selector(text=kml), response.meta)

    return _inner
