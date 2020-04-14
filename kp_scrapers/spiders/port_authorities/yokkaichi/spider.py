import datetime as dt

from scrapy import FormRequest, Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.yokkaichi import normalize, parser


class YokkaichiSpider(PortAuthoritySpider, Spider):
    name = 'Yokkaichi'
    provider = 'Yokkaichi'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://office.yokkaichi-port.or.jp/pls/home/hve010.edit']

    # prevent scrapy from discarding "duplicate" responses
    spider_settings = {'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter'}

    def __init__(self, date_range=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # default to parsing two weeks of forecasted portcalls
        # `date_range` argument : "23/5/2019,6/6/2019"
        date_range = (
            date_range if date_range else f'{parser.get_jst_time()},{parser.get_jst_time(days=14)}'
        )
        self.dates_to_parse = parser.parse_datetime_range(date_range)

    def parse(self, response):
        """Send post request to get data in the future week."""
        self.logger.info('Obtaining data in date range: %s', self.dates_to_parse)

        for day in self.dates_to_parse:
            formdata = {'p_cnt': '1', 'p_date': day, 'p_vessel': ''}
            yield FormRequest(
                url=self.start_urls[0],
                formdata=formdata,
                callback=self.parse_table,
                meta={'date_of_page': day},
            )

    def parse_table(self, response):
        """Parse html table from response.

        Args:
            scrapy.Response:

        Yields:
            Dict[str, str]:

        """
        # memoise reported_date so it won't need to be called repeatedly
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        )

        # date_of_page = response.meta['date_of_page']
        for idx, row in enumerate(response.xpath('//table[@border="1"]/tr')):
            # skip header and previous table
            # first row will always be the header
            if idx == 0:
                header = [' / '.join(th.xpath('./text()').extract()) for th in row.xpath('./th')]
                continue

            cells = [parser.naive_parse_html(cell) for cell in row.xpath('./td').extract()]
            raw_item = {head: cells[head_idx] for head_idx, head in enumerate(header)}
            # contextualise raw item with meta info
            raw_item.update(
                port_name=self.name, provider_name=self.provider, reported_date=reported_date
            )

            # yield raw_item
            yield normalize.process_item(raw_item)
