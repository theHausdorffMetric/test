import re

from kp_scrapers.lib.date import to_isoformat


SUB_TABLE_TITLES = ['VESSELS IN PORT', 'WAITING AT SEA', 'SAILED VESSELS']


def get_reported_date(raw_date):
    """Match and parse a raw reported date to something recognisable.

    To avoid errors due to misspellings, we discard the day name and keep only the numeric date.

    Args:
        raw_date (str):

    Returns:
        str : ISO8601 formatted date

    Examples:
        >>> get_reported_date('WEDNEDAY 14/11/2018')  # [sic]
        '2018-11-14T00:00:00'
        >>> get_reported_date('WEDNEDAY 14 /11/2018')  # [sic]
        '2018-11-14T00:00:00'
        >>> get_reported_date('SUNDAY18/11/2018')
        '2018-11-18T00:00:00'
        >>> get_reported_date('FRD.&SAT.14&15/12/2018')
        '2018-12-15T00:00:00'
        >>> get_reported_date('THURSDAY.10/1/2019')
        '2019-01-10T00:00:00'
        >>> get_reported_date('THURSDAY//30.5.2019')
        '2019-05-30T00:00:00'

    Raises:
        ValueError: if reported date does not match the regex pattern

    """
    match = re.match(r'.*[^0-9](\d{1,2}\s*[^0-9]\s*\d{1,2}\s*[^0-9]\s*\d{4})', raw_date)
    if not match:
        raise ValueError(f"Unknown reported date format: {raw_date}")

    return to_isoformat(match.group(1).replace(' ', ''), dayfirst=True)


def get_table_header(sheet, row_start_idx, row_end_idx):
    """Reconstruct the table header.

    Originally, the table header is separated into three rows (Row 4-6). We need to extract the
    information and return a list of header.

    Args:
        sheet (xlrt.Sheet):
        row_start_idx (int):
        row_end_idx (int):

    Returns:
        List[str]:

    """
    col_num = sheet.row_len(0)
    header = [''] * col_num

    for row in range(row_start_idx, row_end_idx + 1):
        for col in range(col_num):  # col
            header[col] = header[col] + sheet.cell(row, col).value + ' '

    return [cell.strip() for cell in header]


def check_row_type(row):
    """Detect if the row is empty row, data row or sub table title row.

    Port Position table contains three sub tables:
    1. VESSELS IN PORT
    2. WAITING AT SEA
    3. SAILED VESSELS

    Data row check:
    Col 1 is not empty.
    [... number:20.0, ...]

    Sub table title row check:
    Col 10 contains sub table titles
    [empty... text:'WAITING AT SEA', empty...]

    Empty row check:
    Col 1 is empty string (check title row first).

    Args:
        row (xlrt.Sheet):

    Returns:
        str | None:

    """

    data_check = row[1].value

    if data_check != '':
        return 'Data'  # data row

    title_check = row[10].value

    if title_check in SUB_TABLE_TITLES:
        return title_check  # table header row

    return None  # empty row


def map_row_to_dict(headers, row, **additional_info):
    """Transform a data row to a dict object.

    Args:
        headers: raw items' key
        row: raw items' value (table row)
        **additional_info:

    Returns:
        Dict[str, str]:

    """
    raw_item = {header: row[idx] for idx, header in enumerate(headers) if header != ''}
    raw_item.update(additional_info)
    return raw_item
