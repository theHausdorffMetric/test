import datetime as dt

from scrapy import Spider
from scrapy.spiders import Request

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.tokyobay import normalize


class TokyoBaySpider(PortAuthoritySpider, Spider):
    name = 'TokyoBay'
    provider = 'TokyoBay'
    version = '1.1.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel]

    start_urls = [
        # original url: http://www6.kaiho.mlit.go.jp/tokyowan/schedule/URAGA/schedule_3.html
        'https://translate.googleusercontent.com/translate_p?rurl=translate.google.com&sl=auto&sp=nmt4&tl=en&u=http://www6.kaiho.mlit.go.jp/tokyowan/schedule/URAGA/schedule_3.html'  # noqa
    ]

    def parse(self, response):
        """Landing page.

        Args:
            response (Response):

        Yields:
            FormRequest:

        """
        yield Request(
            url=response.xpath('//div[@id="contentframe"]//iframe/@src').extract_first(),
            callback=self.parse_frame,
        )

    def parse_frame(self, response):
        """Entrypoint for parsing website response.

        Args:
            response (scrapy.Response):

        Yields:
            event (Dict[str, str]):

        """
        # memoise reported date since source does not provide any

        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
        )

        # only northbound table is scraped for incoming arrivals
        table = response.xpath("//table//tbody")[0]

        for row in table.xpath(".//tr"):
            row = [
                cell.xpath('.//span[@class="notranslate"]//text()').extract()
                if cell.xpath('.//span[@class="notranslate"]//text()')
                else ''
                for cell in row.xpath('.//td')
            ]
            row = self.format_row(row)

            # the actual headers cannot be mapped in the normalize script, used numbers instead
            # possibly due to encoding issues
            # 'Date and time', 'Type of arrival', 'Ship name', 'Ship type', 'Gross tonnage',
            # 'full length', 'Type', Country of Citizenship,
            # Port of Origin, Destination Port, Warning Ship, pilot
            # header = response.xpath('//table//thead//span[@class="notranslate"]/text()').extract()
            raw_item = {str(idx): may_strip(cell) for idx, cell in enumerate(row)}

            raw_item.update(provider_name=self.provider, reported_date=reported_date)

            yield normalize.process_item(raw_item)

    @staticmethod
    def format_row(raw_row):
        """Format raw list from extracted xpath list. Raw list
        contains the original japanese and english together. Only
        the translated item is required

        Args:
            raw_row (List[str, str]):

        Examples: [
            ['04/06 02:20', ' 04/06 02:20'],
            ['西航船', ' West sailing vessel'],
            ['おれんじ ホープ', ' Inc Hope'],
            ['客船', ' Cruise ship'],
            ['15732', ' 15732'],
            ['179', ' 179'],
            '',
            ['JPN', ' JPN'],
            ['神戸', ' Kobe'],
            ['新居浜', ' Niihama'],
            '',
            ['Ｃ', ' C']
        ]

        Returns:
            List[str]:
        """
        formatted_list = []
        for raw_cell in raw_row:
            if isinstance(raw_cell, list):
                formatted_list.append(raw_cell[1])
            else:
                formatted_list.append(raw_cell)

        return formatted_list
