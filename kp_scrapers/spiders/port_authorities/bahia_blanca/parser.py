import re

from kp_scrapers.lib.utils import remove_diacritics


# configuration for extracting tabular data from PDF
PARSING_OPTIONS_FORECAST_TABLE = {'--stream': [], '--pages': ['2']}
PARSING_OPTIONS_HARBOUR_TABLE = {'--lattice': [], '--pages': ['1']}


def parse_forecast_row(row):
    """

    Examples:  # noqa
        >>> parse_forecast_row(['09/01/2019 CHIPOL CHANGJIANG Hong Kong 188', 'MR', 'D 1.500 MAT. PROYECTO ÉOLICO', 'CHINA', 'CHINA 5 GALVAN'])
        ['09/01/2019', 'CHIPOL CHANGJIANG', 'Hong Kong', '188', 'MR', 'D 1.500 MAT. PROYECTO ÉOLICO', 'CHINA', 'CHINA 5 GALVAN']

    """
    _prefixed_list = _split_date_name_flag_length(row.pop(0))
    row = _prefixed_list + row
    return row


def _split_date_name_flag_length(raw_string):
    """Split a string containing vessel name, flag and length into their separate elements.

    Examples:
        >>> _split_date_name_flag_length('09/01/2019 CHIPOL CHANGJIANG Hong Kong 188')
        ['09/01/2019', 'CHIPOL CHANGJIANG', 'Hong Kong', '188']
        >>> _split_date_name_flag_length('01/02/2019 NADESHIKO Panamá 229')
        ['01/02/2019', 'NADESHIKO', 'Panama', '229']

    Args:
        raw_string (str):

    Returns:
        List[str]:

    """
    pattern = r'(\d{2}\/\d{2}\/\d{4})\s+([A-Z\s]+)\s+([A-Z][a-zA-Z\s]+)\s+(\d{,3})'
    match = re.match(pattern, remove_diacritics(raw_string))
    if not match:
        raise ValueError(f"Unknown string pattern: {repr(raw_string)}")

    return list(match.groups())


def _split_movement_volume_product(raw_string):
    """Split a string containing vessel name, flag and length into their separate elements.

    Examples:
        >>> _split_movement_volume_product('C  15.000/10.000 CEBADA/MALTA')
        {'movement': 'C', 'volume': '15.000/10.000', 'product': 'CEBADA/MALTA'}
        >>> _split_movement_volume_product('D/C  9.000/10.000 NAFTA/GAS OIL')
        {'movement': 'D/C', 'volume': '9.000/10.000', 'product': 'NAFTA/GAS OIL'}
        >>> _split_movement_volume_product('C  13000/9000 PROPANO/BUTANO')
        {'movement': 'C', 'volume': '13000/9000', 'product': 'PROPANO/BUTANO'}
        >>> _split_movement_volume_product('D65.000 CRUDO')
        {'movement': 'D', 'volume': '65.000', 'product': 'CRUDO'}
        >>> _split_movement_volume_product('D 21000 MAIZ')
        {'movement': 'D', 'volume': '21000', 'product': 'MAIZ'}

    Args:
        raw_string (str):

    Returns:
        Dict[str, str]:

    """
    pattern = r'(?P<movement>[CD\/]+)\s*(?P<volume>[\d\.\/]+)\s+(?P<product>.+)'
    match = re.match(pattern, remove_diacritics(raw_string))
    if not match:
        raise ValueError(f"Unknown string pattern: {repr(raw_string)}")

    return match.groupdict()
