import re

from kp_scrapers.lib.parser import may_strip


REGEX_NO_CHARTER_STATUS = r'^([0-9+-/]+)\s([A-z/\- ]+)\s\(([0-9]+)([A-z]+)\)\s([A-z. ]+)\/([A-z.\- ]+)(?:[^A-z0-9]{2,3})([A-z.0-9 ]+)\s([0-9]+K ?.*|WS ?.*|O\/P|[0-9.]+M ?.*|RNR.*)?$'  # noqa
NO_CHARTER_STATUS_HEADERS = [
    'laycan',
    'charterer',
    'volume',
    'cargo',
    'departure_zone',
    'arrival_zone',
    'vessel',
    'rate',
]

REGEX_SHORT = r'^([0-9+-/]+)\s([A-z/\- ]+)\s\(([0-9]+)([A-z]+)\)\s([A-z. ]+)\/([A-z.\- ]+)(?:[^A-z0-9]{2,3})([A-z.0-9 ]+)$'  # noqa
SHORT_HEADERS = [
    'laycan',
    'charterer',
    'volume',
    'cargo',
    'departure_zone',
    'arrival_zone',
    'vessel',
]


def split_row(row):
    """Try to split the row.
        Format 1 (no charter status):
        02-04/02 BP (30FO) SINGAPORE/EC.AUSTRALIA- ALPINE LOYALTY O/P
        26-28/01 MITSUI (80FO) T.PELEPAS/KOREA-JAPAN - STAVANGER EAGLE WS 115
        02-04/02 BP (30FO) SINGAPORE/HONG KONG - ALPINE LOYALTY O/P

        Format 2 (no rate and charter status)
        02-04/02 BP (30FO) SINGAPORE/HONG KONG - ALPINE LOYALTY

    Args:
        row (str):

    Returns:
        Tuple(List[str], List[str]): cells and headers

    """
    # format 1
    no_charter_status_match = re.match(REGEX_NO_CHARTER_STATUS, may_strip(row))
    if no_charter_status_match:
        return list(no_charter_status_match.groups()), NO_CHARTER_STATUS_HEADERS

    # format 2
    short_match = re.match(REGEX_SHORT, may_strip(row))
    if short_match:
        return list(short_match.groups()), SHORT_HEADERS

    return None, None
