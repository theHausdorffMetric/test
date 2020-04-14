from scrapy import Request

from kp_scrapers.spiders.bases.persist import PersistSpider
from kp_scrapers.spiders.operators import OperatorSpider
from kp_scrapers.spiders.operators.extractors.uk import ExcelExtractorUK


BASE_URL = 'https://www.nationalgridgas.com'
DAILY_SEARCH_STRING = 'Daily storage and LNG operator information'
HISTORICAL_SEARCH_STRING = 'Storage and LNG operator information'


class UKOperatorInventoriesSpider(OperatorSpider, PersistSpider):
    """This spider parses Uk national grid storage webpage.

    The website provides one file per year.
    By default, we only got the last one, for the current year.
    If the argument start_date is passed to the spider, we will get and parse all historical files
    until the date specified.

    """

    name = 'UKOperatorInventories'
    provider = 'National Grid'
    version = '0.1.0'

    start_urls = (
        'https://www.nationalgridgas.com/data-and-operations/transmission-operational-data',
    )

    def __init__(self, start_date=None, *args, **kwargs):
        # Get last file only if there is no start date
        self.search_strings = ['Daily storage and LNG operator information']
        if start_date:
            self.search_strings.append('Storage and LNG operator information')

        super().__init__(start_date, *args, **kwargs)

    def parse(self, response):
        for search_string in self.search_strings:
            # Historical search string matches multiple urls
            urls = response.xpath(
                f'//div[./h3[contains(text(), "{search_string}")]]//td/a/@href'
            ).extract()

            for url in urls:
                yield Request(url=BASE_URL + url, callback=self.parse_xls)

    # TODO: JM: should use csv or API
    def parse_xls(self, response):
        xl_obj = ExcelExtractorUK(response.body, response.url, self.start_date)
        return xl_obj.parse_sheets()
