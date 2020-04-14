from kp_scrapers.lib.parser import row_to_dict


def _extract_table(table):
    """Extract tabular data from a standard format as per the website structure.

    Args:
        scrapy.Selector:

    Yields:
        Dict[str, Optional[str]]:

    """
    for idx, row in enumerate(table.xpath('.//tr')):
        if idx == 0:
            # headers will only occur in the first row
            headers = row.xpath('./th/text()').extract()
            continue

        # extract each data row of the table
        yield row_to_dict(row, headers), row.xpath('.//a/@href').extract_first()


def _extract_mgmt_table(response):
    """Extract shipping management data from specially formatted table.

    Args:
        scrapy.Response:

    Returns:
        Dict[str, Optional[str]]:

    """
    keys = response.xpath('//dt//text()').extract()
    # empty html tags are a possibility, hence we iterate over the xpath selector first
    values = [dd.xpath('text()').extract_first() for dd in response.xpath('//dd')]

    return dict(zip(keys, values))
