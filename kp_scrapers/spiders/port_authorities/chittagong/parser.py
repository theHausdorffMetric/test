"""Parsing functions specific to Chittagong spider."""
import logging

from kp_scrapers.lib.parser import may_strip


TABULA_OPTIONS_TYPE = {
    '-p': ['1'],  # pages to extract
    '-g': [],  # guess dimensions of table to extract data from
    '-l': [],  # lattice-mode extraction (more reliable with table borders)
}

TABULA_OPTIONS_BERTHED = {
    '-p': ['all'],  # pages to extract
    '-g': [],  # guess dimensions of table to extract data from
    '-l': [],  # lattice-mode extraction (more reliable with table borders)
}

TABULA_OPTIONS_ETA = {
    '-p': ['all'],  # pages to extract
    '-g': [],  # guess dimensions of table to extract data from
    '-l': [],  # lattice-mode extraction (more reliable with table borders)
}

BERTHED_DATE_AREA = {
    '-p': ['1'],  # pages to extract
    '-a': ['7.605,301.86,78.975,600.58'],  # area parameters to extract from
}

ETA_DATE_AREA = {
    '-p': ['1'],  # pages to extract
    '-a': ['45.045,8.19,89.505,253.89'],  # area parameters to extract from
}


VESSEL_BLACKLIST = ['NO SPACE', 'Out of Commission', 'Dredging']

CARGO_BLACKLIST = [
    'BALLAST',
    'CONT',
    'FOR VISIT',
    'P.CARGO',
    'REPAIR',
    'SCRAPPING',
    'TRAWLER',
    'VEHICLE',
    '-',
]


logger = logging.getLogger(__name__)


def _find_document_type(table_rows):
    first = 0
    for row in table_rows:
        if first < 2:
            # different behaviour on scrapping hub vs local:
            # one empty line added on scrapping hub, special treatment
            _row = [may_strip(cell) for cell in row]
            first = first + 1
            if 'JETTY NO.' == _row[0] or 'SL. NO.' == _row[0]:
                doc_type = _row[0]
                yield doc_type
        else:
            break


def _process_berthed_rows(table_rows):
    for row in table_rows:
        # different behaviour on scrapping hub vs local:
        # one empty line added on scrapping hub, special treatment
        if len(row) < 8:
            continue
        # if multiple tables in the pdf report:
        # the second tab is parsed inconsistently need to correct manually
        # can improve with better tabula option ?
        if row[2] == '':
            row.pop(2)
        if row[4] == '':
            row.pop(4)
        if row[3] == '':
            row.pop(3)
        if any(blacklist in row[1] for blacklist in VESSEL_BLACKLIST) or not row[1]:
            continue
        if any(blacklist in row[3] for blacklist in CARGO_BLACKLIST) or any(
            blacklist in row[4] for blacklist in CARGO_BLACKLIST
        ):
            continue
        # occasionally, 2 row data may be squashed into 1, separated by linebreaks
        if (
            row[8].count('/') > 1 or row[7].count('/') > 1
        ):  # check if more than one date in the cell
            for idx in range(2):
                _row = [
                    cell.split('\n')[idx] if len(cell.split('\n')) > 1 else cell.split('\n')[0]
                    for cell in row
                ]
                _row.append('berthed')
                yield _row
        else:
            _row = [cell.replace('\n', '').replace('\r', '') for cell in row]
            _row.append('berthed')
            yield _row


def _process_eta_rows(table_rows):
    # counter to map tables to type of event
    tables_seen = 0
    for row in table_rows:
        if row[1] == '':
            row.pop(1)
        tables_seen += 1 if row[0] == '1' else 0
        event_type = 'eta' if tables_seen < 3 else 'arrived'
        # 3rd table is 'OUTSIDE PORT LIMIT', discard. But we still want to keep table headers
        if tables_seen == 3 and 'NAME' not in row[1]:
            continue
        # filter irrelevant cargoes in Eta tables
        if event_type == 'eta' and (
            any(blacklist in row[10] for blacklist in CARGO_BLACKLIST)
            or any(blacklist in row[9] for blacklist in CARGO_BLACKLIST)
        ):
            continue
        # filter irrelevant cargoes in arrived tables
        if event_type == 'arrived' and any(blacklist in row[4] for blacklist in CARGO_BLACKLIST):
            continue
        _row = [cell.replace('\n', '').replace('\r', '') for cell in row if cell]
        _row.append(event_type)
        # pdf may join the last two cells and overwrite with irrelevant info, usually useless
        if len(_row) != 13:
            logger.warning(f'discarding inconsistent row {row}')
            continue
        yield _row


def extract_row_headers(table, status):
    """Extract both row and headers from raw 'table'

    Raw table may consist of one table or multiple tables with different table headers.
    This function extracts the raw header(s) and rows depending on number of tables in
    raw 'table'.

    Args:
        table (List[List[str]]): entire table(s) extracted via `extract_pdf_table`

    Yields:
        headers (List[List[str]]): one or more table headers (if there are)
        data_rows (List[List[str]]): table content, either eta or berthed events
        arrived_rows (List[List[str]]): table content, only arrived events

    """
    if status == 'berthing':
        headers = [table[0][:12]]
        data_rows = table[1:]
        arrived_rows = []

    elif status == 'vessel_arriving':
        headers = [row for row in table if 'NAME OF' in row[1]]
        data_rows = [row for row in table[1:] if row[-1] == 'eta']
        arrived_rows = [row for row in table[1:] if row[-1] == 'arrived'][1:]

    return headers, data_rows, arrived_rows
