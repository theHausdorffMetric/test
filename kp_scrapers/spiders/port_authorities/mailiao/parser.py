HEADERS = [
    'ID',
    'POB',
    'PIER',
    'SHIP\'S NAME',
    'Register',
    'DRAFT_0',
    'G/L',
    'M',
    'BT',
    'HP',
    'DRAFT_1',
    'L/P',
    'CARGO',
    'ETA',
    'Unknown',
    'Notice',
    'REMARK',
]


def map_row_to_dict(row, **additional_info):
    """Transform table row into a dict.

    Args:
        row (List[str]):
        **additional_info:

    Returns:
        Dict[str, str]:

    """
    col_num = len(HEADERS)

    if len(row) == col_num:
        raw_item = {HEADERS[idx]: value for idx, value in enumerate(row)}
        raw_item.update(additional_info)
        return raw_item
