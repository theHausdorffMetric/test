import re

from kp_scrapers.lib.parser import may_strip


INFORMATION_REGEX = r'(.*?) FOR ([A-z ]+). ([0-9,]+)([A-z]+) ([A-z ]+). LAYCAN EX ([A-z .]+) ([A-z0-9- ]+). FREIGHT (.*)'  # noqa
HEADERS = ['vessel', 'charterer', 'volume', 'units', 'cargo', 'departure_zone', 'laycan', 'rate']


def split_row(row):
    """Try to split the row.
    CHANGELOS GABRIEL FOR UNKNWON PARTY. 60,000MT FUEL OIL. LAYCAN EX U.S GULF 03-05 DECEMBER.
    FREIGHT UNKNOWN NORDIC GENEVA FOR CLEARLAKE. 60,000MT FUEL OIL. LAYCAN EX TANJUNG PELEPAS 29-31
    DECEMBER. FREIGHT OWN PROGRAM TC

    Args: row (str):

    Returns: Tuple(List[str], List[str]): cells and headers

    """
    match = re.match(INFORMATION_REGEX, may_strip(row))
    if match:
        return list(match.groups()), HEADERS

    return None, None
