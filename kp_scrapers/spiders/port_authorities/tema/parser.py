import xlrd

from kp_scrapers.lib.date import to_isoformat


def get_xlsx_sheet(response):
    """Obtain workbook sheet containing relevant data from response.

    Args:
        response (scrapy.Response):

    Returns:
        xlrd.sheet:

    """
    # get relevant table and headers
    return xlrd.open_workbook(file_contents=response.body, on_demand=True).sheet_by_index(0)


def _extract_reported_date(raw_date_str):
    """Extract reported date from spreadsheet's given raw string.

    Args:
        raw_date_str (str):

    Returns:
        str: ISO8601-formatted date string

    Examples:
        >>> _extract_reported_date('DAILY PORT SITUATION FROM 25TH JULY 2018 TO 27TH JULY 2018') # noqa
        '2018-07-25T00:00:00'

    """
    return to_isoformat(raw_date_str.split('PORT SITUATION FROM')[1].split('TO')[0])


def _map_row_to_dict(row, header, **key_values):
    """Map a list with a header to a dictionary.

    TODO could be made generic

    Args:
        row (List[str]):
        header (List[str]):
        **key_values: additional key-value pairs to append to dict

    Returns:
        Dict[str, str]:

    """
    raw_item = {head: row[idx] for idx, head in enumerate(header)}
    raw_item.update(**key_values)
    return raw_item
