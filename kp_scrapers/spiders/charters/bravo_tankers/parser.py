import logging

from kp_scrapers.lib.parser import may_strip


logger = logging.getLogger(__name__)

SEPARATOR = '\xa0'
ROW_LENGTH = 6
MIN_ROW_LENGTH = 2


def map_row_to_dict(row, **additional_info):
    """Transform row to dict with headers as key.

    Args:
        row (List[str]):
        **additional_info:

    Returns:
        Dict[str, str]:

    """
    raw_item = {str(idx): cell for idx, cell in enumerate(row)}
    raw_item.update(additional_info)
    return raw_item


def get_data_table(body):
    """Restore the data table from raw html.

    Args:
        body (Selector):

    Returns:

    """
    table = body.xpath('//span[@lang="IT"]/text()').extract()
    for raw_row in table:
        row = [may_strip(x) for x in raw_row.split(SEPARATOR) if may_strip(x)]

        if len(row) == ROW_LENGTH:
            yield row

        if MIN_ROW_LENGTH < len(row) < ROW_LENGTH:
            logger.error(f'Do we miss this record? {row}')
