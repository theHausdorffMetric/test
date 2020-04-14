import re

from kp_scrapers.lib.parser import may_strip


PATTERN_HEADER_MAPPING = {
    # Vessel/Charterer         Cargo Voyage             Laycan      Rate          Status
    # MAERSK PENGUIN/WINSON-90 ULSD SIKKA/UKC-SPORE      06 SEP     1.9M-W92.5    FXD.
    # JAG LOKESH/BP-90         ULSD RUWAIS/UKC-SPORE-OZ  29-31 AUG  RNR           FXD.
    'full_row': (
        (
            r'([\w\.\s]+)/'
            r'([A-Z\s]+-[\d.]+)\s'
            r'([A-Z+]+)\s'
            r'([A-Z\s\-/]+)'
            r'(\d{1,2} [A-Z]{3,4}|\d{1,2}-\d{1,2} [A-Z]{3,4})\s'
            r'([\w\.\-]+)\s'
            r'(FXD|ON HOLD|ON SUBS|SUBS)'
        ),
        ['vessel', 'charterer', 'cargo', 'voyage', 'lay_can', 'rate_value', 'status'],
    ),
    # Vessel-Departure Zone          Laycan     Status
    # ENERGY CENTURION – FUJAIRAH    01 SEP     FXD.
    # PACIFIC JULIA-KARACHI          06 SEP     ON HOLD.
    'short_row': (
        (
            r'([\w\.\s]+)[–-]{1}'
            r'([A-Z\s]+)'
            r'(\d{1,2} [A-Z]{3,4}|\d{1,2}-\d{1,2} [A-Z]{3,4})\s'
            r'(FXD|ON HOLD|ON SUBS|SUBS)'
        ),
        ['vessel', 'departure_zone', 'lay_can', 'status'],
    ),
    # Vessel/Charterer     Cargo    Voyage      Laycan  Voyage        Rate    Status
    # MARIBEL/ST-55        NAP      AG/JAPAN    02 SEP  SIKKA/JAPAN   W100    SUBS.
    'extra_voyage': (
        (
            r'([\w\.\s]+)/'
            r'([A-Z\s]+-[\d.]+)\s'
            r'([A-Z+]+)\s'
            r'([A-Z\s\-/]+)'
            r'(\d{1,2} [A-Z]{3,4}|\d{1,2}-\d{1,2} [A-Z]{3,4})\s'
            r'([A-Z\s\-/]+?)'
            r'([\w\.\-]+)\s'
            r'(FXD|ON HOLD|ON SUBS|SUBS)'
        ),
        ['vessel', 'charterer', 'cargo', 'voyage', 'lay_can', 'voyage_1', 'rate_value', 'status'],
    ),
    # Vessel/Charterer         Voyage             Laycan      Rate          Status
    # LAKE STURGEON OOS/ATC-60 YANBU/JEDDAH       05 SEP      230K          SUBS.
    'full_row_without_cargo': (
        (
            r'([\w\.\s]+)/'
            r'([A-Z\s]+-[\d.]+)\s'
            r'([A-Z\s\-/]+)'
            r'(\d{1,2} [A-Z]{3,4}|\d{1,2}-\d{1,2} [A-Z]{3,4})\s'
            r'([\w\.\-]+)\s'
            r'(FXD|ON HOLD|ON SUBS|SUBS)'
        ),
        ['vessel', 'charterer', 'voyage', 'lay_can', 'rate_value', 'status'],
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

    return None, None
