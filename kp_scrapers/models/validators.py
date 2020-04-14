import datetime as dt

from schematics.exceptions import ValidationError

from kp_scrapers.business import MIN_YEAR_SCRAPED
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.spiders.ais import safe_imo


def is_valid_imo(candidate):
    if not safe_imo(candidate):
        raise ValidationError('Vessel IMO number is invalid')
    return candidate


def is_positive_number(candidate):
    """Useful when not much assumptions is needed but yet validate we don't have garbage.

    Examples:
        >>> is_positive_number(34)
        34
        >>> is_positive_number('34')
        '34'
        >>> is_positive_number(-1) # doctest:+IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ValidationError: ["-1 is not a positive integer"]
        >>> is_positive_number('foo') # doctest:+IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ValidationError: ["number is invalid: foo"]

    """
    if try_apply(candidate, float):
        # NOTE there could be an option to include 0. But most cases just don't want to
        if float(candidate) > 0:
            return candidate
        raise ValidationError(f'{candidate} is not a positive number')

    raise ValidationError(f'number is invalid: {candidate}')


def is_valid_range(candidate, min_value, max_value):
    """Generic range checker with type tolerance."""
    if try_apply(candidate, int):
        if int(candidate) >= min_value <= max_value:
            return candidate
        raise ValidationError('range is not within {} to {} tons'.format(min_value, max_value))

    raise ValidationError('range is invalid: {}'.format(candidate))


def is_valid_build_year(candidate):
    """Check if the given build year makes sense.

    Args:
        candidate(str|int):

    Examples:
        >>> is_valid_build_year(1978)
        1978
        >>> is_valid_build_year('2020')
        '2020'
        >>> is_valid_build_year('foo') # doctest:+IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ValidationError: ["range is invalid: foo"]
        >>> is_valid_build_year(1789) # doctest:+IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ValidationError: ["range is not within 1942 to 2018"]

    """
    # some website can actually provide insignts of the near future, like coming new builds
    # yet `3` is quite an empirical value
    return is_valid_range(candidate, MIN_YEAR_SCRAPED, dt.datetime.now().year + 3)


def is_valid_cargo_movement(candidate):
    """Check if the given cargo movement can only be "discharge" or "load".

    Args:
        candidate (str):

    Examples:
        >>> is_valid_cargo_movement('discharge')
        'discharge'
        >>> is_valid_cargo_movement('export') # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ValidationError: ["Cargo movement must be either `discharge` or `load`"]
        >>> is_valid_cargo_movement(None) # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ValidationError: ["Cargo movement must be either `discharge` or `load`"]

    """
    if candidate not in ['discharge', 'load']:
        raise ValidationError('Cargo movement must be either `discharge` or `load`')
    return candidate


def is_valid_numeric(candidate):
    """Check if the given string is an absolute numeric.

    Args:
        candidate (str | int | float):

    Examples:
        >>> is_valid_numeric('4500')
        '4500'
        >>> is_valid_numeric(4500)
        4500
        >>> is_valid_numeric('-4500') # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ValidationError: ["Volume/Speed must be an absolute value"]
        >>> is_valid_numeric('foobar') # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ValidationError: ["Volume/Speed is not a numeric: foobar"]

    """
    if not try_apply(candidate, float, int):
        raise ValidationError('Value is not numeric: {}'.format(candidate))
    if float(candidate) < 0:
        raise ValidationError('Value must be an absolute value')
    return candidate


def is_valid_rate(candidate):
    """Check if the rate value is valid.

    Args:
        candidate (str):

    Returns:

    """
    if try_apply(candidate, float):
        return candidate

    raise ValidationError(f'Rate value is invalid: {candidate}')
