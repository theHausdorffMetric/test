import re

from kp_scrapers.lib.parser import may_strip


MISSING_ROWS = []
IGNORE_ROWS = [
    '----',
    'CARIBS/SOUTHAMERICA',
    'VESSEL SIZE L/C ROUTE RATE CHARTERERS',
    'EX WEST',
    'WEST AFRICA',
    'INDO/FAR EAST',
]


PATTERN_HEADER_MAPPING = {
    'cpp_row': (
        (
            r'^(.*)\s'
            r'(\d{2,})\s'
            r'(.*)\s'
            r'(\d{1,2}\/[A-z]+|\d{1,2}\-\d{1,2}\/[A-z]+)\s'
            r'(.*)\s'
            r'(WS.*?|RNR.*?|COA.*?|USD.*?|[0-9.\s\/]+M|[0-9.\s\/]+K)\s'
            r'(.*)$'
        ),
        [
            'vessel_status',
            'cargo_volume',
            'cargo_product',
            'lay_can',
            'voyage',
            'rate_value',
            'charterer',
        ],
    ),
    'dpp_row': (
        (
            r'^(.*)\s'
            r'(\d{2,})\s'
            r'(\d{1,2}\/\d{1,2}|\d{1,2}\-\d{1,2}\/\d{1,2})\s'
            r'(.*)\s'
            r'(WS.*?|RNR.*?|COA.*?|USD.*?|[0-9.\s\/]+M|[0-9.\s\/]+K)\s(.*)$'
        ),
        ['vessel', 'cargo_volume', 'lay_can', 'voyage', 'rate_value', 'charterer_status'],
    ),
}


def split_row(row):
    """Try to split the row.

    Args:
        row (str):

    Returns:
        Tuple(List[str], List[str]): cells and headers

    """
    for desc, value in PATTERN_HEADER_MAPPING.items():
        pattern, headers = value

        match = re.match(pattern, may_strip(row))
        if match:
            return list(match.groups()), headers
    if row and not any(sub in row for sub in IGNORE_ROWS):
        MISSING_ROWS.append(row)
    return None, None
