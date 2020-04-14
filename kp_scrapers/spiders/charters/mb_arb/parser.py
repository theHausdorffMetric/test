import re

from kp_scrapers.lib.parser import may_strip


FULL_ROW = 'full'
SHORT_ROW = 'short'
FAILED_ROW = 'FAILED'

SHARED_CELL_IDX = 4


def get_data_table(body):
    """Get data table from html.

    Args:
        body (Selector):

    Returns:
        List[List[str]]:

    """
    prev_row = None
    table = []
    for raw_row in body.xpath('//text()').extract():
        row, desc = try_match(may_strip(raw_row))

        if desc is FULL_ROW:
            prev_row = row

        if desc is SHORT_ROW:
            # get shared info from previous row
            # add them to current row (short row)
            row = prev_row[:SHARED_CELL_IDX] + row

        if desc is FAILED_ROW:
            table.pop()

        if row:
            table.append(row)

    return table


def try_match(row):
    """Try to match the row with given regular expression and split the cells.

    Row format 1:
        GEM NO. 2 | 260CRUDE | 22-27/09 | URUGUAY/CHINA | RNR | SHELL

    Row format 2 (share row info with previous line):
        EC.MEXICO+USG/KOREA | 5.05M | HYUNDAI

    Args:
        row (List[str]):

    Returns:
        Tuple[List[str], str]:

    """
    pattern_mapping = {
        FULL_ROW: (
            r'([A-Z0-9./\s\-]+?)\s??'
            r'(\d+)'
            r'([A-z]+)\s'
            r'([0-9]{1,2}/[0-9]{1,2}|[0-9]{1,2}-[0-9]{1,2}/[0-9]{1,2})\s'
            r'([A-Z+./\s\-]+)\s'
            r'(RNR|O/P|COA|WS [0-9.]+|[0-9.]+M)\s'
            r'([A-Z-!?()/\s]+)$'
        ),
        SHORT_ROW: (r'([A-Z+./\s\-]+)\s(RNR|O/P|COA|WS [0-9.]+|[0-9.]+M)\s([A-Z-!?()/\s]+)$'),
    }

    if re.search(FAILED_ROW, row):
        return None, FAILED_ROW

    for desc, pattern in pattern_mapping.items():
        match = re.match(pattern, row)
        if match:
            return list(match.groups()), desc

    return None, None
