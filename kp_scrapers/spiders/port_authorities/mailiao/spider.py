import re

from scrapy import Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.mailiao import normalize, parser


class MailiaoSpider(PortAuthoritySpider, Spider):
    name = 'Mailiao'
    provider = 'Mailiao'
    version = '2.0.1'
    produces = [DataTypes.PortCall]

    reported_date = None
    start_urls = [
        'http://dss.rffpcc.com.tw/DSS_REAL/VIEW.aspx',
        # to interpret cargo code: http://crm3.fpg.com.tw/mlharbor/Cc1h03r.do
    ]

    def parse(self, response):
        """Generate eta events from Mailiao Port.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        # get reported date and normalize it first
        self.reported_date = normalize.normalize_reported_date(
            response.xpath('//span[@id="appTXT"]/text()').extract_first()[-11:]
        )

        # get table data
        raw_text = response.xpath('//div[@id="GridCallBack"]/script[last()]/text()').extract_first()
        text_match = re.search(r'Grid1\.Data = (.*);\nGrid1\.Levels', raw_text)
        if not text_match:
            raise ValueError(f'Unable to find relevant table data:\n{raw_text}')

        # in the form of list as a string representation
        for row in eval(text_match.group(1)):
            raw_item = parser.map_row_to_dict(row, **self.meta_fields)
            if not raw_item:
                continue

            yield normalize.process_item(raw_item)

    @property
    def meta_fields(self):
        return {
            'port_name': self.name,
            'provider_name': self.provider,
            'reported_date': self.reported_date,
        }
