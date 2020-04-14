import logging

from kp_scrapers.lib.parser import may_strip


logger = logging.getLogger(__name__)


def extract_reported_date(response):
    """Extract reported date of page at time of scraping.

    Args:
        response (scrapy.HtmlResponse):

    Returns:
        str:

    """
    return may_strip(response.xpath('//span[@class="style17"]/text()').extract_first())


def extract_table_and_headers(response):
    """Extract the relevant table, as well its headers.

    Args:
        response (scrapy.Selector):

    Returns:
        scrapy.Selector, List[str]: table, headers

    """
    table = extract_table(response)
    headers = extract_headers(table, rm_headers=['(m)'])
    return extract_table(response), headers


def extract_table(response):
    """Extract relevant table from website.

    Args:
        response (scrapy.HtmlResponse):

    Returns:
        scrapy.Selector:

    """
    # relevant data is only in the second table
    return response.xpath('//table')[1]


def extract_headers(table, rm_headers=[]):
    """Extract headers of specified table.

    NOTE could be made generic

    Args:
        table (scrapy.Selector):
        rm_headers (List[str]): list of header names to remove before returning

    Returns:
        List[str]:

    """
    headers = table.xpath('.//tr[@class="omg"]//font/text()').extract()
    # remove unneccessary headers
    return [may_strip(head) for head in headers if head not in rm_headers]


def extract_rows_from_table(table):
    """Extract a list of rows from table selector.

    Args:
        table (scrapy.Selector):

    Returns:
        List[scrapy.Selector]:

    """
    # we discard the last row since it's just general regulatory info
    return table.xpath('.//tr[not(@class="omg")]')[:-1]
