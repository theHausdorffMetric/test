import re

from kp_scrapers.lib.parser import may_strip


PATTERN_HEADER_MAPPING = {
    # 'ADAFERAÂ Â Â Â Â Â Â Â Â Â GLASFORDÂ Â 11-13 APRÂ Â 100,000MT JINZHOUÂ Â Â Â Â 405K'
    # 'OCEAN PEGASUSÂ Â Â Â Â ?Â Â Â Â Â Â Â Â Â 03-04 APRÂ Â 100,000MT'
    # 'FENG HUANG ZHOUÂ Â Â CNOOCÂ Â Â Â Â 08-09 APRÂ Â 100,000NT HUIZHOUÂ Â Â Â Â RNR'
    'unusual_delimiter': (
        (
            r'^([A-Z\d\-\s]+)Â[Â\s]*'  # vessel name
            r'([A-Z\?\/\-\s]*)[Â\s]*'  # charterer (may be missing)
            r'([\d\-]+\s[A-Z]+)[Â\s]*'  # laycanq
            r'([A-Z\d,]+)[Â\s]*'  # quantity
            r'([A-Z\-\.]*)[Â\s]*'  # arrival zone (may be missing)
            r'([A-Z\d\s\.\/]*)$'  # rate value (may be missing)
        ),
        ['vessel_name', 'charterer', 'laycan', 'quantity', 'destination', 'rate'],
    ),
    # 'EBN BATUTA SHELL 25-28 JAN 100,000MT P.DICKSON 480K'
    # 'HS MEDEA VITOL 04-07 JAN 100,000MT'
    # 'NINGBO DAWN SHELL 01-04 JAN 100,000MT OPTIONS'
    # 'TOKYO MARU COSMO/JX 30-02 FEB 100,000MT JAPAN O/P'
    'normal_delimiter': (
        (
            r'^([A-z\d\s]+)[\s]'  # vessel name
            r'([A-Z\?\/\.]+)[\s]'  # charterer
            r'([\d\-]+\s[A-Z]+)[\s]*'  # laycanq
            r'([A-Z\d,]+)[\s]*'  # quantity
            r'([A-Z\-\.]*)[\s]*'  # arrival zone (may be missing)
            r'([A-Z\d\s\.\/]*)$'  # rate value (may be missing)
        ),
        ['vessel_name', 'charterer', 'laycan', 'quantity', 'destination', 'rate'],
    ),
}


def parse_raw_text(row):
    """Parse raw text based on observed regex pattern.

    Delimiters between columns are "Â", which may differ in quantity and whitespaces.

    Args:
        text (str):

    Returns:
        Tuple[str] | None: tuple if regex matched, else None

    Examples:  # noqa
        >> parse_raw_text('ADAFERAÂ Â Â Â Â Â Â Â Â Â GLASFORDÂ Â 11-13 APRÂ Â 100,000MT JINZHOUÂ Â Â Â Â 405K')
        ('ADAFERA', 'GLASFORD', '11-13 APR', '100,000MT', 'JINZHOU', '405K')
        >> parse_raw_text('FENG HUANG ZHOUÂ Â Â CNOOCÂ Â Â Â Â 08-09 APRÂ Â 100,000NT HUIZHOUÂ Â Â Â Â RNR')
        ('FENG HUANG ZHOU', 'CNOOC', '08-09 APR', '100,000NT', 'HUIZHOU', 'RNR')
        >> parse_raw_text('OCEAN PEGASUSÂ Â Â Â Â ?Â Â Â Â Â Â Â Â Â 03-04 APRÂ Â 100,000MT')
        ('OCEAN PEGASUS', '?', '03-04 APR', '100,000MT', '', '')
        >> parse_raw_text('TOKYO MARU COSMO/JX 30-02 FEB 100,000MT JAPAN O/P')
        ('TOKYO MARU', 'COSMO/JX', '30-02 FEB', '100,000MT', 'JAPAN', 'O/P')
    """
    for desc, value in PATTERN_HEADER_MAPPING.items():
        pattern, headers = value

        match = re.match(pattern, may_strip(row))
        if match:
            return list(match.groups()), headers

    return None, None
