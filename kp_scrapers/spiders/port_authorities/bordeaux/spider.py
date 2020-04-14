import datetime as dt

from scrapy import FormRequest, Spider

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.bordeaux import normalize, parser


class BordeauxSpider(PortAuthoritySpider, Spider):
    name = 'Bordeaux'
    provider = 'Bordeaux'
    version = '1.0.2'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['https://www.bordeaux-port.fr/fr/navires-quais-attendus']

    form_types = ['1', '2']  # docked vessels  # expected arrival of vessels

    def parse(self, response):
        """Dispatch response to corresponding parsing function depending on URL.

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            scrapy.FormRequest:

        """
        for form_type in self.form_types:
            yield FormRequest(
                url='https://www.bordeaux-port.fr/info/ajax_callback/block',
                method='POST',
                formdata={'type_infos': form_type},
                callback=self.parse_form_response,
                # store the reported date now, since the POST response below
                # will not have the reported date in the body
                meta={'reported_date': self.extract_reported_date(response)},
            )

    def parse_form_response(self, response):
        """Parses form response from POST, and extract raw item from it.

        Args:
            response (scrapy.HtmlResponse):

        Returns:
            Dict[str, str]:

        """
        for table in parser.extract_table_selector(response):
            raw_item = parser.extract_raw_item(
                table,
                port_name=self.name,
                provider_name=self.provider,
                reported_date=response.meta['reported_date'],
            )
            yield normalize.process_item(raw_item)

    def extract_reported_date(self, response):
        """Extract data from table as a raw item.

        Args:
            response (scrapy.HtmlResponse):

        Returns:
            str

        """
        date_str = response.xpath(
            '//body/div[3]/div[4]/div/div/div[1]/section[2]/div/p[3]/text()'
        ).extract_first()
        if not date_str:
            self.logger.error('No reported date found, default to current locale time')
            return (
                dt.datetime.utcnow()
                .replace(hour=0, minute=0, second=0)
                .isoformat(timespec='seconds')
            )

        return to_isoformat(date_str.replace('Last update: ', ''), dayfirst=True)
