import datetime as dt

from scrapy import Request, Spider

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.nagoya import normalize


# dock name as key for illustration, it will be easier for later reference
_DOCK_CODE_MAPPING = {
    'ガーデンふ頭': '13',
    '築地・築地東ふ頭': '03',
    '大手ふ頭': '17',
    '稲永ふ頭': '04',
    '潮凪ふ頭': '05',
    '大江ふ頭': '06',
    '昭和ふ頭': '21',
    '船見ふ頭': '12',
    '新宝ふ頭': '19',
    '空見ふ頭': '22',
    '潮見ふ頭': '20',
    '東海元浜ふ頭': '24',
    '横須賀ふ頭': '07',
    '北浜ふ頭': '16',
    '南浜ふ頭': '18',
    '浮標（内港）': '10',
    '金城ふ頭': '14',
    '木場金岡ふ頭': '08',
    '飛島ふ頭': '23',
    '浮標（西部）': '11',
    '弥富ふ頭': '25',
    '鍋田ふ頭': '15',
    '錨地（４Ｂ）': '09',
}


class NagoyaSpider(PortAuthoritySpider, Spider):
    name = 'Nagoya'
    version = '1.0.0'
    provider = 'Nagoya'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    # forecast: http://www2.port-of-nagoya.jp/select/selpierarrival.aspx
    # in-port:  http://www2.port-of-nagoya.jp/select/selpierexist.aspx
    start_urls = [
        # vessels forecast
        'http://www2.port-of-nagoya.jp/select/selarrivallist.aspx?PageCd={port_code}',
        # vessels in-port (TODO we don't scrape it for now since it does not bring much value)
        # 'http://www2.port-of-nagoya.jp/select/selexistlist.aspx?PageCd={port_code}',
    ]

    reported_date = (
        dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
    )

    # on our platforms, the zone name is actually Yokkaichi, not Nagoya
    port_name = 'Yokkaichi'

    def start_requests(self):
        """Entry point of Nagoya spider.

        Yields:
            scrapy.Request:

        """
        for url in self.start_urls:
            for dock_name, dock_code in _DOCK_CODE_MAPPING.items():
                yield Request(
                    url=url.format(port_code=dock_code),
                    callback=self.parse,
                    meta={'dock': dock_name},
                )

    def parse(self, response):
        """Parse forecast/in-port activity of vessels at specified dock.

        Args:
            scrapy.Response:

        Yields:
            Dict[str, str]:

        """
        reported_date = response.xpath('//h2/text()').extract_first()
        is_data_available = response.xpath('//th')

        # there is no recorded activity currently at the specified dock
        if not is_data_available:
            return

        for idx, row in enumerate(response.xpath('//tr')):
            # headers will always be at the first row of the table
            if idx == 0:
                headers = row.xpath('.//th/text()').extract()
                continue

            raw_item = row_to_dict(row, headers)
            # contextualise raw item with dock name and meta info
            raw_item.update(
                installation=response.meta['dock'],
                port_name=self.port_name,
                provider_name=self.provider,
                reported_date=reported_date,
            )

            yield normalize.process_item(raw_item)
