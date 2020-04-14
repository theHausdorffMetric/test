import re

from kp_scrapers.lib.parser import may_strip


MISSING_ROWS = []


PATTERN_HEADER_MAPPING = {
    # Aframax attachment
    # (.*)\s([0-9]+)\s(.*?)\s([0-9\-A-z]+\/[0-9]+)\s(.*?)\s?\/\s(.*?)\s(W[0-9\.\-]+|RNR|\$[0-9A-z]+)\s(.*)
    # CELSIUS ESBJERG 80 FO 04-06/12 SINGAPORE / N CHINA W130 FREEPOINT
    # ADAFERA OOS 80 FO 05-07/12 STS KAZ / SIKKA-EAST W215-180 ST SHIPPING
    'aframax': (
        (
            r'(.*)\s'
            r'([0-9]+)'
            r'(kb|KB)?'
            r'(.*?)\s'
            r'([0-9\-A-z]+\/[0-9]+)\s'
            r'(.*?)\s?\/\s'
            r'(.*?)\s'
            r'(WS[0-9\.\-: RNR\$\/MmKk]+|W[0-9\.\-: RNR]+|RNR|RNR[W0-9\.\-: ]+|\$[: 0-9A-z\.]+)\s'
            r'(.*)'
        ),
        [
            'vessel',
            'volume',
            'unit',
            'cargo',
            'lay_can',
            'departure',
            'arrival',
            'rate',
            'charterer',
        ],
    ),
    # Green Point 200kb Dco 19-20/11 Balongan / Singapore Freepoint
    # Maersk Malaga 40 Fo 03-05/12 Singapore / N China Linkoil
    # ^(.*)\s([0-9]+)\s(.*?)\s([0-9\-A-z]+\/[0-9]+)\s(.*?)\s\/\s(.*?)$
    'panamax_mr': (
        (
            r'^(.*)\s'
            r'([0-9]+)'
            r'(kb|KB)?'
            r'(.*?)\s'
            r'([0-9\-A-z]+\/[0-9]+)\s'
            r'(.*?)\s\/\s'
            r'(.*?)$'
        ),
        ['vessel', 'volume', 'unit', 'cargo', 'lay_can', 'departure', 'arrival_charterer'],
    ),
}


def split_row(row):
    """Try to split the row.

    Args:
        row (str):

    Returns:
        Tuple(List[str], List[str]): cells and headers

    """
    row = may_strip(row.replace('(OLD)', '').replace('(REPLACE)', '(UPDATED RATE)'))
    for desc, value in PATTERN_HEADER_MAPPING.items():
        pattern, headers = value

        match = re.match(pattern, row)
        if match:
            return list(match.groups()), headers
    if row and not (
        row.lower().startswith('ex ') or row.lower().startswith('-') or len(row.split('-')) == 3
    ):
        MISSING_ROWS.append(row)

    return None, None
