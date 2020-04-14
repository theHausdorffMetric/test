from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.units import Unit


def extract_activity_detail_rows(response):
    """Extract relevant vessel and arrival info from detailed activity page.

    This page has two tables, we'll need the first table.

    Args:
        response (scrapy.HtmlResponse):

    Returns:
        List[scrapy.Selector]: table data as rows

    """
    return response.xpath('//table[1]//tr')


def extract_cargo_movement_rows(response):
    """Extract relevant cargo info from detailed activity page.

    The second table of the page contains cargo movement info.

    `raw_cargo` in this function comes in these format:
        - [<RECEIVER>, <MOVEMENT>, <PRODUCT>, <VOLUME>] (one product onboard)
        - [<MOVEMENT>, <PRODUCT>, <VOLUME>] (one product onboard)
        - [<R>, <M>, <P>, <V>, <R>, <M>, <P>, <V>] (two product onboard)
        - [<M>, <P>, <V>, <M>, <P>, <V>] (two product onboard)

    Args:
        response (scrapy.HtmlResponse):

    Returns:
        Dict[str, str]:

    """
    raw_cargo = [
        cell
        for cell in response.xpath('//table[2]//tr[position()>2]//text()').extract()
        if cell != '\n'
    ]

    for idx, cell in enumerate(raw_cargo):
        if cell in ['Sbarco', 'Imbarco']:
            yield {
                'movement': cell,
                'product': raw_cargo[idx + 1],
                'volume': raw_cargo[idx + 2] if len(raw_cargo) >= idx + 3 else None,
                'volume_unit': Unit.tons,
            }


def extract_raw_from_rows(rows, **additional_data):
    """Extract relevant table rows from detailed activity page.

    Args:
        rows (List[scrapy.Selector]): table data as rows
        **key_values (Dict[str, str]): dictionary of additional data we want to append to raw item

    Returns:
        List[str]: table data as rows

    """
    raw_item = {}
    for row_selector in rows:
        raw_row = _remove_elements(
            lst=row_selector.xpath('.//td//text()').extract(), rm_elements=['\xa0']
        )
        raw_item.update(_transform_single_raw_to_dict(raw_row))

    # append additional key-value pairs
    raw_item.update(additional_data)

    return raw_item


def _transform_single_raw_to_dict(raw_row):
    """Transform a list to a dict.

    Each raw list will always have the key name as the first element.
    Hence, we only return a dict mapping if the list contains more than one element.

    Args:
        raw_row (List[str]):

    Returns:
        Dict[str, str]:

    """
    # rows with length more than one contain key value pairs, else we discard such rows
    if len(raw_row) <= 1:
        return {}
    # we use the first element as the header key
    # then we join the other elements together as a key-value
    return {raw_row[0]: ' '.join(raw_row[1:])}


def _remove_elements(lst, rm_elements=[]):
    """Remove useless elements of a list

    NOTE could be made generic

    Args:
        lst (List[str]):
        rm_elements (List[str]): list of elements to remove before returning

    Returns:
        List[str]:

    """
    # remove unneccessary elements
    for rm_element in rm_elements:
        stripped_lst = [element for element in lst if element != rm_element]
    return [may_strip(element) for element in stripped_lst if element != '\n']
