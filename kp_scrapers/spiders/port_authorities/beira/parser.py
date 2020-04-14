import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip


DATE_TYPE_IDX = 0
VESSEL_IDX = 1
CARGO_IDX = 10

COMMON_HEADERS = [
    'VESSEL',
    'QUAY',
    'DRAFT',
    'IMP/EXP',
    'LOA',
    'TYPE',
    'AGENT',
    'CARRIER',
    'TERMINAL',
    'TNS',
]

logger = logging.getLogger(__name__)


def filter_rows(table):
    """Filter empty and irrelevant rows, only keep data and header rows.

    Args:
        table_rows (List[List[str]]):

    Yields:
        str: reported date str
        Dict[str, str]: table data rows

    """
    for idx, row in enumerate(table):
        row = [may_strip(cell) for cell in row if cell]
        # discard useless, irrelevant rows
        if len(row) <= 2:
            continue

        # reported date is always in the first row
        if idx == 0:
            yield row[0]
            continue

        # extract headers
        if row[1] == 'VESSEL':
            headers = row
            continue

        yield {headers[idx]: cell for idx, cell in enumerate(row)}


def parse_reported_date(raw_date):
    """Converts raw date to ISO-8601 format string.

    Args:
        str:

    Returns:
        str: ISO-8601 date string

    Examples:  # noqa
        >>> parse_reported_date('MOCAMBIQUE, S.A.\\nBEIRA BRANCH/SHIPPING DEPT.\\n\\nPORT POSITION   10th December 18')
        '2018-12-10T00:00:00'

    """
    _match = re.search(r'PORT POSITION\s*(.*)', raw_date)
    if not _match:
        raise ValueError(f'Unable to parse reported date: {repr(raw_date)}')

    return to_isoformat(_match.group(1), dayfirst=True)


def parse_table(raw_table):
    """Parse table and map row into dict.

    We use common headers field here is due to the header might not be stable, and we want the first
    cell to correctly parse the event type. It happens quite often that there's missing border in
    header row, therefore we would miss field or get combined fields. For example:

                                ______         _________
                      ........ | TYPE | AGENT | CARRIER |........
                               --------------------------

    The 'AGENT' field would be missing as there's no upper border.

    We could also use sub table name to identify the event type, but it's also unstable due to the
    text size is larger and it's bold style, for `TCC 8 ANNOUNCED` sub table, we would get
    `TCC 8 ANNUNCED` instead. Therefore, we choose the simplest method to implement header
    retrieval.


    Args:
        raw_table (List[List[str]]):

    Yields:
        Dict[str, str]:

    """
    headers = None
    for row in raw_table:
        # cleanup and remove empty strings since they can skew the below interpretations
        row = [elem for elem in row if elem]
        if len(row) < len(COMMON_HEADERS):
            continue

        # header row, retrieve date type to assemble header
        if row[VESSEL_IDX] == 'VESSEL':
            if row[DATE_TYPE_IDX]:
                headers = [row[DATE_TYPE_IDX]] + COMMON_HEADERS
                continue

            # first column is empty
            else:
                logger.error(f'Lack date type to assemble the header: {row}')
                return

        if headers and len(row) > CARGO_IDX and row[VESSEL_IDX] and row[CARGO_IDX]:
            yield {headers[idx]: cell for idx, cell in enumerate(row)}
