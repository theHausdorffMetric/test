import re


MISSING_ROWS = []


def return_list(raw_row):
    """clean and get a consistent pattern, insert serperator

    raw row is intially a string
    """
    # remove spaces in the date fields
    row = re.sub(r'\s+\-\s+', ' - ', raw_row).strip()
    # replace multiple spaces with a seperator to split into list
    row = re.sub(r'\s{2,}', '|', row).strip().split('|')
    return row


def validate_list(raw_list):
    """sometimes the number and vessel are fused together

    """
    _list = return_list(raw_list)
    if len(_list) == 7:
        try:
            # number and vessel row fused
            filler, vessel_name = _list[0].split(' ', 1)
            _list[0] = vessel_name
            _list.insert(0, filler)
        except Exception:
            return _list

    return _list
