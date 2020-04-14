import logging


HEADER_ROW_SIGN = 'Vessel'
RELEVANT_SECTION = ['LPG', 'Petchems']
IDENTIFY_IDX = 0

logger = logging.getLogger(__name__)


def process_table(table):
    """Process table and map row to dict.

    Args:
        table (List[List[str]]):

    Yields:
        Dict[str, str]:

    """
    processing = False
    current_section = 'LPG'
    headers = None
    row_processed_count = 0
    for row in table:
        # retrieve headers from table
        # because there might be joint row situation
        if _is_header_row(row):
            processing = True
            headers = row
            continue

        # detect next section so that we only process relevant sections
        # current_section is initialized to `LPG`
        # it's not very stable but since we're unable to detect first portion
        # so it's a compromise
        if processing and _is_section_row(row):
            current_section = row[IDENTIFY_IDX]
            continue

        # process non-empty relevant data row
        empty_check = processing and headers and row[IDENTIFY_IDX]
        relevant_check = current_section in RELEVANT_SECTION
        # why is the condition len(row)>7 here?
        # In few rare cases tabula jar in SHUB doesn't read the pdf the same way it reads in local
        # this causes the footer rows to get append to the valid rows list and it
        # logs "header and row are insconsitesnt" error message. This small sanity check will
        # prevent this. In most cases the minimum columns were 7, tats why 7.
        if empty_check and relevant_check and len(row) > 7:
            if _sanity_check(headers, row):
                row_processed_count += 1
                yield {headers[idx]: cell for idx, cell in enumerate(row)}

            else:
                logger.error('Headers and row are inconsistent, need manual check.')
                return

    # provide visibility for the relevant row processed numbers
    logger.info(f'Processed {row_processed_count} relevant rows in this report.')


def _sanity_check(headers, row):
    """Simple sanity check that header and row are consistent.

    We have this check for the reason that the extracted header:
        ['Vessel', 'cbm', 'mts Cargo', '', 'Load', 'Disch', 'Laycan', 'Rate', 'Charterer']
    while the row:
        ['Gas Pride', '9,000', '4,500', 'PGP', 'Al Jubail', 'Yanbu', '15-16 Sep', 'RNR', 'Sabic']

    Inconsistency is in mts and Cargo field.

    Args:
        headers (List[str]):
        row (List[str]):

    Returns:
        Boolean:

    """

    return True if len(headers) == len(row) else False


def _is_section_row(row):
    """Detect if the row is section row.

    If it's section row, all the cells are empty except the first one.

    Args:
        row (List[str]):

    Returns:
        Boolean

    """
    if row[0] and all(not cell for cell in row[1:]):
        return True

    return False


def _is_header_row(row):
    """Detect if the row is header row.

    Args:
        row (List[str]):

    Returns:
        Boolean

    """
    return row[0].startswith(HEADER_ROW_SIGN)
