from collections import OrderedDict  # noqa

from scrapy import Request

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.customs import CustomsSpider
from kp_scrapers.spiders.customs.united_states import normalize


_PURPOSE_ENTRANCE = 'entrance'
_PURPOSE_CLEARANCE = 'clearance'


class USCustomsSpider(PdfSpider, CustomsSpider):
    """Scrape import/export from USCustoms pdf.

    Website publishes a new PDF every friday with data from the previous week.
    Historicals pdf are not accessible on the website.

    You can find historical file in google drive folder 'Record of Vessels in Foreign Trades'
    https://drive.google.com/drive/folders/0B0u3ZmzI16ZQbUlPU3ZmUkF1aU0

    """

    name = 'USCustoms'
    version = '2.0.0'
    # FIXME as of now, DB stores both `USCustomsClearance` and `USCustomsEntrance` when loading
    provider = 'USCustoms'
    produces = [DataTypes.CustomsPortCall]

    start_urls = [
        'https://www.cbp.gov/document/forms/cf-1401-record-vessel-foreign-trade-clearances',
        'https://www.cbp.gov/document/forms/cf-1400-record-vessel-foreign-trade-entrances',
    ]

    _tabula_options = {'--lattice': [], '--pages': ['all']}

    def parse(self, response):
        """Entrypoint of USCustoms spider."""
        xpath = (
            '//*[@id="block-views-news-publications-block-4"]'
            '/div/div/div/div/div/span/span/span/a/@href'
        )
        pdf_url = response.xpath(xpath).extract_first()
        if not pdf_url:
            raise ValueError("Unable to acquire USCustoms resource")

        yield Request(pdf_url, callback=self.parse_pdf)

    def parse_pdf(self, response):
        # depending on file obtained, customs declaration will describe different vessel movements
        _source = self._determine_source(response.url)

        table = self.extract_pdf_io(response.body, use_dict_reader=True, **self._tabula_options)
        for row in table:
            # strip empty keys
            raw_item = {may_strip(k): v for k, v in row.items() if k}
            # contextualise raw item with meta info
            raw_item.update(source=_source, provider_name=f'{self.provider}{_source.title()}')
            yield normalize.process_item(raw_item)

    def _determine_source(self, url):
        """Try and find a valid statement describing vessels filed in resource.

        Args:
            url (str):

        Returns:
            str:

        """
        url = url.lower()
        # https://www.cbp.gov/document/forms/cf-1401-record-vessel-foreign-trade-clearances
        if '1401' in url or _PURPOSE_CLEARANCE in url:
            return _PURPOSE_CLEARANCE

        # https://www.cbp.gov/document/forms/cf-1400-record-vessel-foreign-trade-entrances
        elif '1400' in url or _PURPOSE_ENTRANCE in url:
            return _PURPOSE_ENTRANCE

        else:
            raise ValueError(f"Unable to determine source from `{url}`")
