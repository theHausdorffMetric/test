import re


def extract_volume_product_date(raw_item):
    """
    Parse the raw item to get volumne and product

    Args:
        raw_item (list[str])

    Returns:
        list[str]
    """
    # index 2 here contains the field volume and product combined
    # the re will seperate them out and add it to the list
    vol_product = raw_item[2]
    raw_item.append(re.findall(r'[0-9.,]*', vol_product)[0])
    raw_item.append(re.findall(r'[^\s\d][A-Z]*', vol_product)[0])
    return raw_item
