import logging


HEADER_SIGN = 'VESSEL'
HEADER_FIELDS = ['VESSEL', 'CARGO', 'POL', 'POD', 'LAYCAN', 'FREIGHT', 'ACCOUNT']

logger = logging.getLogger(__name__)

MISSING_ROWS = []


def process_table(table):
    """Process table, map valid row to dict and collect invalid row as missing row.

    We have multiple table, therefore multiple headers, for each table, we want to check if the
    cell is split correctly, meaning, no joint columns.

    So the strategy here would be:
        1. Check for header row, if it's valid, go ahead, if not, set header as None, in this case,
           the relevant rows will be collected
        2. Although the header row doesn't join together, the row could be joint, therefore we also
           check if the header and row are consistent.

    Args:
        table List[List[str]]:

    Yields:
        Dict[str, str]:

    """
    start_processing = False
    headers = None
    process_count = 0
    for row in table:
        if _is_header_row(row):
            if is_valid_header(row):
                headers = row
                start_processing = True

            else:
                headers = None
                logger.warning(f'Slitted header is invalid: {row}')
            continue

        if start_processing and not is_section_row(row):
            if headers and _sanity_check(headers, row):
                process_count += 1
                yield {headers[idx]: cell for idx, cell in enumerate(row)}

            else:
                logger.warning(f'Header and row are inconsistent, headers: {headers}, row: {row}')
                MISSING_ROWS.append(' '.join(row))

    logger.info(f'Processed {process_count} rows of fixtures.')


def _sanity_check(headers, row):
    """Simple sanity check that header and row are consistent.

    We need this check because we spotted inconsistency between headers and row:
    Header:
        ['VESSEL', '', 'CARGO', 'POL', 'POD', 'LAYCAN', 'FREIGHT', 'ACCOUNT']
    Row:
        ['EPIC BURANO', '4,000 MT BUT', '', 'CORUNNA', 'AGADIR', 'OCT 17/19', 'RNR', 'REPSOL']

    Args:
        headers (List[str]):
        row (List[str]):

    Returns:
        Boolean:

    """
    header_sign = [True if cell else False for cell in headers]
    row_sign = [True if cell else False for cell in row]
    return all(header_sign[idx] == x for idx, x in enumerate(row_sign))


def _is_header_row(row):
    """Detect if it's header row.

    Args:
        row (List[str]):

    Returns:
        Boolean:

    """
    return HEADER_SIGN in row[0]


def is_valid_header(row):
    """Detect if it's valid header (all the fields are correctly separated).

    Args:
        row (List[str]):

    Returns:
        Boolean:

    """
    return [cell.strip() for cell in row if cell] == HEADER_FIELDS


def is_section_row(row):
    """Detect if the row is section.

    Args:
        row (List[str]):

    Returns:
        Boolean:

    """
    if row[0] and all(not cell for cell in row[1:]):
        return True

    return False
