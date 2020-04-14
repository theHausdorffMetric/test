# -*- coding: utf-8 -*-

"""Useful methods to help to safely parsing web chaos."""

from __future__ import absolute_import
import functools
import json
import re

from scrapy.selector import Selector
import six


# Occidental encoding for Latin alphabet, digits, punctuation and control.
OCCIDENTAL_ENCODING = 'ISO-8859-1'
HTTP_OK = 200


def may_strip(dangerous_str):
    """Try to defensively remove dirty spaces.

    Example:
        >>> may_strip(None)
        >>> may_strip(4)
        >>> may_strip(' UNION  SAPPHIRE    ')
        'UNION SAPPHIRE'
        >>> # workons on unicode too (contrary to str.strip)
        >>> may_strip(u' UNION  SAPPHIRE    ')
        'UNION SAPPHIRE'

    """
    return ' '.join(dangerous_str.split()) if isinstance(dangerous_str, six.string_types) else None


def may_apply(value, *transforms):
    """Wrapper trying to cast the given value if it makes sense.

    Args:
        cast(function): a callable taking one argument and returning one value
        value(*): any data to pass to the callable

    Returns:
        (*): whatever type `cast(value)` returned, or None if value is None

    Example:
        >>> may_apply(5.6, int)
        5
        >>> may_apply(5.6, str)
        '5.6'
        >>> may_apply('5.6', str)
        '5.6'
        >>> may_apply('5.0', float, int)
        5
        >>> may_apply(None, int)

    """
    for transform in transforms:
        value = str(value).replace('\n', '') if '\n' in str(value) else value
        value = transform(value) if value is not None else None
    return value


def try_apply(value, *data_types):
    """Try and cast a value to a desired data type.

    Args:
        value (*): any value
        *data_types (List[type]): desired data types to be applied in order

    Returns:
        * | None: casted data type if supported, else None

    Examples:
        >>> try_apply('alpha', float)
        >>> try_apply('5.6', float)
        5.6
        >>> try_apply('5.6', list)
        ['5', '.', '6']
        >>> try_apply(None, int)

    """
    try:
        for data_type in data_types:
            value = data_type(value)
        return value
    except (TypeError, ValueError):
        return None


def extract_nth_cell(line, nth):
    """Extract table cell.

    TODO: `nth-child` is currently used over 60 times, migrate them to this method.
    TODO: proper testing

    Args:
        line(scrapy.Selector): html selector on a table row

    Returns:
        str: content of the targeted cell

    """
    q = 'td:nth-child({}) *::text'.format(nth)
    return ''.join(line.css(q).extract()).strip()


def fast_extract_nth_cell(line, nth):
    """Extract table cell.

    This method is faster than extract_nth_cell when there is lot of cells in a
    row. But it was not extensively tetsted on existing spiders.

    Args:
        line(scrapy.Selector): html selector on a table row
        i: integer, cell column number

    Returns:
        str: content of the targeted cell

    """
    rows = line.css('td *::text')
    if len(rows) < nth:
        # we have less results than the requested cell
        return ''

    return rows[nth - 1].extract() or ''


def row_to_dict(line, header, **key_values):
    """Naively try to transform html table row to structured dict.

    Setting empty cells to `None` works nicely with other parser helpers (like
    `may_apply` above) and modelisation efforts.

    !! NOTICE !! this method needs the header items to be in the same order as
    the line cells, otherwise columns and values will be mixed up.

    Args:
        line(scrapy.Selector): raw row html as parsed by Scrapy
        header(List): table columns, probably extracted before
        **key_values (dict[str, str]): additional key-value pairs to append to dict

    Returns:
        (dict): header/cells mapping

    """
    # empty cells are replaced by `None` for more consistent falsy representation
    raw_item = {key: extract_nth_cell(line, idx + 1) or None for idx, key in enumerate(header)}
    # append additional key-value pairs, if any
    raw_item.update(key_values)

    return raw_item


def may_remove_substring(name, blacklist):
    """Generic function to try and remove blacklisted substrings from a complete string.

    This function will remove dirty spaces if it successfully removed blacklisted substrings.
    It will not remove dirty spaces if it fails to remove any substrings. This is by design,
    in order to maintain input-output identity if no substrings are removed, i.e. we want to
    return the original string with dirty spaces and all.

    Args:
        name (str): offending string
        blacklist (List[str]): list of substrings to remove from `name`

    Returns:
        str:

    Examples:
        >>> may_remove_substring('HMS Zee', ['HMS', 'RRS'])
        'Zee'
        >>> may_remove_substring('   HMS    Zee', ['HMS'])
        'Zee'
        >>> may_remove_substring('HMS Zee', ['USS'])
        'HMS Zee'
        >>> may_remove_substring('   HMS    Zee', ['USS'])
        '   HMS    Zee'
        >>> may_remove_substring('HMS Zee', [])
        'HMS Zee'

    """
    stripped_name = name
    # try and remove specified substrings
    for bad_str in blacklist:
        stripped_name = stripped_name.replace(bad_str, '')
    # we only want to remove dirty spaces if we successfully stripped substrings
    # this is so as to maintain input-output identity should no substrings be removed
    return may_strip(stripped_name) if stripped_name != name else name


def split_by_delimiters(raw_str, *delimiters):
    """Split raw string if there exists any delimiter.

    This function will split by all delimiters it finds.

    Args:
        raw_str (str):

    Returns:
        List[str] | None: split list of strings

    Examples:
        >>> split_by_delimiters('LCO+Gasoline /Diesel, Naphtha & Condensate')
        ['LCO', 'Gasoline', 'Diesel', 'Naphtha', 'Condensate']
        >>> split_by_delimiters('LCO+Gasoline /Diesel, Naphtha & Condensate', '+', '&')
        ['LCO', 'Gasoline /Diesel, Naphtha', 'Condensate']
        >>> split_by_delimiters(None)

    """
    if not raw_str:
        return None

    # default delimiters
    if not delimiters:
        delimiters = [',', '/', '+', '&']

    delimiters = ('\\{}'.format(s) for s in delimiters)
    return [may_strip(single) for single in re.split('|'.join(delimiters), raw_str)]


def str_to_float(str, smart=False):
    try:
        return float(str)
    except ValueError:
        if smart is True and (str is None or str == ''):
            return 0

        return None


def serialize_response(protocol):
    """Boilerplate to pre-load Scrapy callback responses."""

    def _decorator(func):
        @functools.wraps(func)
        def _inner(klass, response):
            # TODO check at response.statusinn [OK_HTTP]
            if protocol == 'jsono' or protocol == 'json':
                return func(klass, json.loads(response.body_as_unicode()))
            elif protocol == 'xml':
                return func(klass, Selector(response))
            else:
                raise NotImplementedError('unknown protocol serializer: {}'.format(protocol))

        return _inner

    return _decorator


# TODO: NCA: cut/pasted from etl.extraction.ais.vesselTrackerAPIPositions
def extract_xpath_data(xpath):
    if len(xpath) > 0:
        res = xpath[0].extract()
        if len(res) > 0:
            return res

    return None
