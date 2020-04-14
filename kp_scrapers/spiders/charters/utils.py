# -*- coding: utf-8 -*-

EAST_ZONES = ['east', 'feast', 'f.east']


def parse_arrival_zones(raw_arrival_zone):
    """Split the raw arrival zones into a list of arrival zones, separated by '-'.

    Args:
        raw_arrival_zone (str):

    Returns:
        list(str)

    """
    raw_arrival_zones = raw_arrival_zone.split('-')
    raw_arrival_zones_treated = []
    for zone in raw_arrival_zones:
        if any(zone.lower() == east_zone for east_zone in EAST_ZONES):
            raw_arrival_zones_treated.append('Eastern Asia')
        else:
            raw_arrival_zones_treated.append(zone)
    return raw_arrival_zones_treated


def create_voyage_raw_text(departure, arrival):
    """From the origin and arrival zones, create the voyage raw text on the following form:
    departure/arrival.

    Args:
        departure (str):
        arrival (str):

    Returns:
        str

    Examples:
        >>> create_voyage_raw_text('foo', 'bar')
        'foo/bar'
        >>> create_voyage_raw_text('foo', ['france', 'spain'])
        'foo/france-spain'

    """
    # in case we receive several arrivals instead of a single string
    if isinstance(arrival, list):
        arrival = '-'.join(arrival)

    return '/'.join([departure, arrival])


def parse_rate(rate, quantity=1):
    """Try to parse human conventions for charter rates.

    Glossary:
        - PD: Per Day
        - WS: World Scale

    Examples:
        List of supported fomrmats.

        >>> parse_rate('17K PD')
        '17K PD'
        >>> parse_rate('550k')
        550000.0
        >>> parse_rate('1.85M')
        1850000.0
        >>> parse_rate('3.3 M')
        3300000.0
        >>> parse_rate('435000lumpsum')
        435000.0
        >>> parse_rate('USD 1,86M')
        1860000.0
        >>> parse_rate('WS40')
        40.0
        >>> parse_rate('WS 137,5')
        137.5
        >>> parse_rate('USD 70 PT', 1000)
        70000.0
        >>> # '-' or '/' seperated values mean it will depend from the
        >>> # destination, hence we cannot decide at this point
        >>> parse_rate('WS58.75/60.75')

        >>> parse_rate('USD12,5MT')

        >>> parse_rate('RNR')

        >>> parse_rate('OWN PROG')

        >>> parse_rate('WS25-23(CC/SS)')

        >>> parse_rate('US$1.8-2.3M')

    Args:
        rate (str):
        quantity (int):

    Return:
        float

    """
    # don't process
    # TODO investigate why we don't need to process this one
    # TODO: ev: we should add tests for this function
    if 'PD' in rate:
        return rate

    # we can't do much with that (RNR means Rate Not Reported for rxample, and
    # we don't yet support USD12,5MT)
    if rate in ['RNR', 'COA', 'OWN PROG', 'OWN'] or 'MT' in rate:
        return None

    # clean up and normalize
    rate = rate.upper().strip().replace(' ', '')

    # Remove currency, since we distinguish rates and dollar prices using the order of magnitude.
    # do it before the rate_coeff choice since 'M' is in 'LUMPSUM'.
    for currency in ['LUMPSUM', 'US$', 'USD']:
        rate = rate.replace(currency, '')

    coeff = _choose_rate_coeff(rate, quantity) or 1.0
    # Now that we have the rate coefficient, remove coeff substring.
    for coeff_string in ['PT', 'M', 'K']:
        rate = rate.replace(coeff_string, '')

    if rate.startswith('W'):
        rate = _clean_ws_rate(rate)

    if '-' in rate or '/' in rate:
        return None

    # finally, the actual parsing
    return to_digit(rate) * coeff


def to_digit(value):
    """"Parse a numerical string to return a float.

    Args:
        value(str): human style float. See example for supported format

    Returns:
        float: parsed float value

    Raises:
        ValueError: If the given str wasn't a float

    Examples:
        Here is a list of illustrations of the format supported.

        >>> to_digit('3.3')
        3.3
        >>> to_digit('1,2')
        1.2
        >>> to_digit('3,3 M')
        3300000.0
        >>> to_digit('4k')
        4000.0
        >>> to_digit('foo') # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        ValueError: could not convert string to float: FOO

    """
    # remove space and normalize digit symbol
    value = value.upper().replace(' ', '').replace(',', '.')

    if 'M' in value:
        return float(value.replace('M', '')) * 1e6
    elif 'K' in value:
        return float(value.replace('K', '')) * 1e3
    else:
        return float(value)


def _clean_ws_rate(rate):
    for dirty in ['WS', 'W', '/RNR', '(CC/SS)']:
        rate = rate.replace(dirty, '')

    # TODO probably need to check the use-case and the implementation
    if ',' in rate and '/' in rate:
        # case: WS 32/35.5, 35/38.5 => 32/38.5
        partials = rate.split(',')
        partials = [r.split() for r in partials]
        partials = [item for r in partials for item in r]
        rate = '/'.join([partials[0], partials[-1]])

    return rate


def _choose_rate_coeff(rate, quantity=None):
    """Convert human-formatted numeric abbreviations.

    Args:
        rate (str): human-like numeric
        quantity (int): ?????? (@seb)

    Returns:
        float: numeric translation of the given raw input

    Example:

        >>> _choose_rate_coeff('30K')
        1000.0
        >>> _choose_rate_coeff('30M')
        1000000.0
        >>> # special cases
        >>> _choose_rate_coeff('unknown')
        >>> _choose_rate_coeff('whateverPT', quantity=4.0)
        4.0

    """
    if 'PT' in rate:
        return quantity
    elif 'M' in rate:
        return 1e6
    elif 'K' in rate:
        return 1e3

    # give up if we don't understand the input
    return None
