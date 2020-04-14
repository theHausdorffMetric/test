import re


# to decide whether to keep a row
VESSEL_NAME_COL_IDX = 4


def get_reported_date(response):
    """Get reported date from pdf file name.

    Extract table name list from response first. The list contains some blank rows, making it hard
    to detect the idx of date. First filter out blank rows, the first row contains date information.

    We extract date from file name, usually the file name would be like:
    1. EXPECTED ARRIVALS AS ON 29TH JULY 2018.pdf
    2. BERTH PLAN AS ON 29TH JULY 2018.pdf

    Args:
        response (Response):

    Returns:
        str:

    """
    # extract <a> contains reported date
    table_name_list = response.xpath('//a[@class="rsfiles-file "]//text()').extract()

    # filter blank rows and get the first row
    first_table_name = [i for i in table_name_list if not re.search(r'\r\n\t', i)][0]

    date_list = first_table_name.split('.')[0].split()[-3:]
    return ' '.join(date_list)


def filter_rows_for_expected_arrival(raw_table, required_col):
    """Filter out rows that required column is empty string.

    Args:
        raw_table (List[List[str]]):
        required_col (int):

    Yields:
        List[str]:

    """
    stop_processing = False
    for idx, row in enumerate(raw_table):
        # table has ended, stop processing
        if stop_processing:
            return

        # filter out irrelevant rows
        if len(row) <= required_col or row[required_col] == '' or row[required_col] == 'SHIP NAME':
            continue

        # this string denotes the end of the report
        if 'CONTAINER SHIP' in ''.join(row):
            stop_processing = True
            continue

        yield row


def map_row_to_dict_for_expected_arrival(raw_row, **additional_info):
    """Map row to dict object.

    As the table is unstable, last two columns will join together, we decide to discard some cols
    to simply the logic.

    The table header:
    DATE | DRAFT | LOA | GRT | SHIP NAME | DISCHARGE | LOAD | AGENT / CARGO / RECEIVER

    By right the header should look like above, but it's quite often the LOAD and last field would
    miss a column line and be joint when extracted. Therefore, we discard DISCHARGE and LOAD info.
    Another reason for us to discard them in this table, is that the data row of DISCARD and LOAD
    would sometimes combined.

    In short, we only keep the first five columns and last field.


    Args:
        raw_row (List[str]):
        **additional_info:

    Returns:
        Dict[str, str]:

    """
    # get the idx of last field
    # we do this because last non-empty cell is the actual last field
    # rather than last cell
    last_field_idx: int
    for idx in range(-1, -len(raw_row), -1):
        if raw_row[idx] != '':
            last_field_idx = idx
            break

    # keep only first five columns and last field
    row = raw_row[: VESSEL_NAME_COL_IDX + 1]
    row.append(raw_row[last_field_idx])

    raw_item = {str(idx): cell for idx, cell in enumerate(row)}
    raw_item.update(additional_info)

    return raw_item


def extract_at_berth_table(raw_table):
    """Extract INFORMATION ON SHIPS AT BERTH table from raw table.

    The table has fixed row number (from 3 to 19) and column span 10. First extract this portion,
    and get the index of last column (cargo column index), because some rows may not start at the
    first column (the first column is empty). We extract the table by extracting column from cargo
    column minus column span (cargo column idx - 10).

    Args:
        raw_table (List[List[str]]):

    Returns:
        List[List[str]]:

    """
    vessel_name_idx = 1
    row_start = 3
    row_end = 19
    col_end = 10

    for idx, row in enumerate(raw_table[row_start:row_end]):
        # remove the first empty string
        if not row[0]:
            del row[0]
            raw_table[idx] = row

        # check if vessel is present, if not, discard
        if row[vessel_name_idx]:
            yield row[:col_end]


def extract_anchorage_table(raw_table):
    """Extract DRIFTING/WAITING SHIPS AT OUTER ANCHORAGE from raw table.

    Usually the column begins at 10, ends at the corresponding column is empty string. The row
    begins at 20, ends at which rows are all empty strings.

    FIXME: row and column index may change.

    Args:
        raw_table (List[List[str]]):

    Returns:
        List[List[str]]:

    """
    # define the portion of anchorage table
    row_start = 20
    row_end = -8
    col_start = 10

    # discard empty rows
    return [row[col_start:] for row in raw_table[row_start:row_end] if row[col_start] != '']


def validate_anchorage_headers(headers):
    """Validate headers of anchorage table, because same header name TIME exists.

    Args:
        headers (List[str]):

    Returns:
        List[str]:

    """

    idx = headers.index('TIME')
    headers[idx] = 'TIME0'

    return headers
