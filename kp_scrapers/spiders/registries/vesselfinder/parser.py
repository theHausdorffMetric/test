def extract_vessel_attributes(response):
    """Extract vessel info.

    Args:
        response (scrapy.Response):

    Returns:
        Dict[str, str]:

    """
    raw_item = {}
    for pair in response.xpath('//tr'):
        key = pair.xpath('.//td[@class="n3"]//text()').extract_first()
        value = pair.xpath('.//td[@class="v3"]//text()').extract_first()
        if key and value:
            raw_item.update({key: value})

    return raw_item
