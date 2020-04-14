import json

from scrapy.http import HtmlResponse
from w3lib.html import remove_tags


IRRELEVANT_HEADERS = [' ', '-']

KEY_VALUE_DELIM = ' : '


def init_dom_object(body, url='', encoding='utf-8', **kwargs):
    """Initialise a DOM to allow easy traversal of nodes using Scrapy's XPath/CSS selectors.

    TODO this function is rather useful and could be made generic.

    Args:
        document (str | unicode): (X)HTML/XML document as string
        url (str | unicode): URL to associate with the response, may be left blank
        encoding (str | unicode): document encoding
        **kwargs:

    Returns:
        scrapy.HtmlResponse:

    """
    return HtmlResponse(url=url, body=body, encoding=encoding, **kwargs)


def extract_table_selector(response):
    """Parse response from POST request, to obtain selector of relevant data

    Args:
        response (scrapy.HtmlResponse):

    Yields:
        scrapy.Selector:

    """
    post_response = init_dom_object(body='\n'.join(json.loads(response.body)), url=response.url)
    # extract list of all tables (each table corresponds to a single vessel)
    yield from post_response.xpath('//table[@class="table"]')


def extract_raw_item(table, **metadata):
    """Extract data from table as a raw item.

    Args:
        table (scrapy.Selector):
        **metadata (dict[str | str]): additional metadata to be appended to raw item

    Returns:
        dict

    """
    # remove html tags
    # we do this instead of using xpath/css selectors since the selector
    # does not return empty strings. we need empty strings to map to a dict's key value.
    table = list(map(remove_tags, table.xpath('.//td').extract()))

    # vessel name will always be the first element of `table`
    # all other elements have their header name and field value delimited by " : " string,
    # the below modification brings it in line with the other elements we have
    table[0] = 'Vessel : ' + table[0]

    # filter junk strings from table and convert to raw item format
    raw_item = dict(
        field.split(KEY_VALUE_DELIM)
        for field in list(filter(None, table))
        if field not in IRRELEVANT_HEADERS
    )

    # append additional metadata, if any
    for meta in metadata:
        raw_item[meta] = metadata[meta]

    return raw_item
