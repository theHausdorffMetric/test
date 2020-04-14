import logging

from kp_scrapers.lib.parser import try_apply


logger = logging.getLogger(__name__)


def parse_header(header):
    """Mutate a header list of combined cells and separate the cells.

    Args:
        row (List[str]):

    Returns:
        List[str]:

    Examples:  # noqa
        >>> parse_header(['Cargo No. Vessel', 'Dates', 'Supp/Rcvr', 'Charterer', 'Grades', 'QTY', 'Next Port', 'Notes'])
        ['Cargo No.', 'Vessel', 'Dates', 'Supp/Rcvr', 'Charterer', 'Grades', 'QTY', 'Next Port', 'Notes']
    """
    if 'vessel' in header[0].lower() and 'cargo' in header[0].lower():
        logger.debug(f'Header list has combined cells, going to separate: {header}')
        _first, _, _second = header[0].rpartition(' ')
        header.insert(0, _first)
        header[1] = _second

    return header


def parse_data_row(row):
    """Mutate a data list of combined cells and separate the cells.

    Args:
        row (List[str]):

    Returns:
        List[str]:

    Examples:  # noqa
        >>> parse_data_row(['403 ADVANTAGE ATOM', '14.04.2018', 'SHELL M/FL', 'SHELL', '600', 'TROLL', 'GDANSK', ''])
        ['', 'ADVANTAGE ATOM', '14.04.2018', 'SHELL M/FL', 'SHELL', '600', 'TROLL', 'GDANSK', '']
        >>> parse_data_row(['TBA EAGLE BARENTS', '14.04.2018', 'STATOIL', 'STATOIL', '600', 'REB', 'BROFJORDEN', ''])
        ['', 'EAGLE BARENTS', '14.04.2018', 'STATOIL', 'STATOIL', '600', 'REB', 'BROFJORDEN', '']
        >>> parse_data_row(['IMPORTS DELTA PIONEER', '14.04.2018', 'STATOIL', 'STATOIL', '600', 'REB', '', 'STS/KTK EAGLE BARENTS'])
        ['IMPORTS', 'DELTA PIONEER', '14.04.2018', 'STATOIL', 'STATOIL', '600', 'REB', '', 'STS/KTK EAGLE BARENTS']
        >>> parse_data_row(['507', 'GHIBLI', '25.05.2018', 'SHELL', 'STATOIL', '600', 'TROLL', 'KALUNDBOIRG', ''])
        ['', 'GHIBLI', '25.05.2018', 'SHELL', 'STATOIL', '600', 'TROLL', 'KALUNDBOIRG', '']
        >>> parse_data_row(['IMPORT', 'BODIL KNUTSEN', '04.05.2018', 'STATOIL', 'STATOIL', '1000', 'STATFJORD', '', 'STS/KTK MARE DORICUM'])
        ['IMPORT', 'BODIL KNUTSEN', '04.05.2018', 'STATOIL', 'STATOIL', '1000', 'STATFJORD', '', 'STS/KTK MARE DORICUM']
        >>> parse_data_row(['605 TBN', '18.06.2018', 'EQUINOR', 'TBN', '600', 'TROLL', 'TBN'])
        ['', 'TBN', '18.06.2018', 'EQUINOR', 'TBN', '600', 'TROLL', 'TBN']
        >>> parse_data_row(['SPARTO', '17.12.2017', 'TOTAL', 'CNR', 'OSEB', '600', 'WHITEGATE'])
        ['SPARTO', '17.12.2017', 'TOTAL', 'CNR', 'OSEB', '600', 'WHITEGATE']
    """
    _movement, _, _vessel = row[0].partition(' ')
    if not _vessel:
        logger.debug(f'Data list either has no combined cells, or has no first column: {row}')
        # we need to determine if the row either has no combined cells, or no first column
        # string will be processed differently depending on the content of the first column
        row[0] = _clean_import_str(_movement) if _has_movement(row[0]) else row[0]
        return row

    if _has_import_pattern(row):
        logger.debug(f'Data list has combined cells, going to separate: {row}')
        row.insert(0, _clean_import_str(_movement))
        row[1] = _vessel

    return row


def _has_import_pattern(row):
    """Check if a row has import pattern cells.

    Args:
        row (List[str]):

    Returns:
        bool: True is row has combined cells

    Examples:  # noqa
        >>> _has_import_pattern(['403 ADVANTAGE ATOM', '14.04.2018', 'SHELL M/FL', 'SHELL', '600', 'TROLL', 'GDANSK', ''])
        True
        >>> _has_import_pattern(['TBA EAGLE BARENTS', '14.04.2018', 'STATOIL', 'STATOIL', '600', 'REB', 'BROFJORDEN', ''])
        True
        >>> _has_import_pattern(['IMPORT DELTA PIONEER', '14.04.2018', 'STATOIL', 'STATOIL', '600', 'REB', '', 'STS/KTK EAGLE BARENTS'])
        True
        >>> _has_import_pattern(['507', 'GHIBLI', '25.05.2018', 'SHELL', 'STATOIL', '600', 'TROLL', 'KALUNDBOIRG', ''])
        True
        >>> _has_import_pattern(['IMPORT', 'BODIL KNUTSEN', '04.05.2018', 'STATOIL', 'STATOIL', '1000', 'STATFJORD', '', 'STS/KTK MARE DORICUM'])
        True
        >>> _has_import_pattern(['605 TBN', '18.06.2018', 'EQUINOR', 'TBN', '600', 'TROLL', 'TBN'])
        True
        >>> _has_import_pattern(['SPARTO', '17.12.2017', 'TOTAL', 'CNR', 'OSEB', '600', 'WHITEGATE'])
        False
    """
    _movement, _, _vessel = row[0].partition(' ')
    return _has_movement(_movement)


def _clean_import_str(cell):
    """Clean a string containing an import pattern.

    Args:
        cell (str):

    Returns:
        bool: True if string has an import pattern

    Examples:
        >>> _clean_import_str('IMPORTS')
        'IMPORTS'
        >>> _clean_import_str('Import')
        'Import'
        >>> _clean_import_str('TBA')
        ''
        >>> _clean_import_str('SPARTO')
        ''
    """
    return cell if 'import' in cell.lower() else ''


def _has_movement(cell):
    """Check if a cell contains a cargo movement pattern.

    Args:
        cell (str):

    Returns:
        bool: True if string has a movement pattern

    Examples:
        >>> _has_movement('TBA')
        True
        >>> _has_movement('IMPORTS')
        True
        >>> _has_movement('403')
        True
        >>> _has_movement('SPARTO')
        False
    """
    return (
        any(each in cell.lower() for each in ['tba', 'import']) or try_apply(cell, int) is not None
    )
