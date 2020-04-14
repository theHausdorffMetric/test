import datetime as dt

from scrapy import FormRequest, Spider

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.taichung import normalize, parser


class TaichungSpider(PortAuthoritySpider, Spider):
    name = 'Taichung'
    provider = 'Taichung'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel]

    start_urls = ['http://aps163.tchb.gov.tw/VB1410/English30DaysShipStatus.php']

    def start_requests(self):
        """Request Taichung portcalls.

        NOTE for now, hardcoded to obtain portcalls in the future

        Yields:
            scrapy.FormRequest:

        """
        filter_date = dt.datetime.today().strftime('%Y%m%d')
        formdata = {
            'DateRange': 'Default',
            'NameCond1': 'DATE_IN',
            'Operator1': '>=',
            'DataCond1': filter_date,
            'Operator12': '',
            'NameCond2': '',
            'Operator2': '',
            'DataCond2': '',
            'SortCond1': 'DATE_IN',
            'vQstr40': 'RunAllShip',
            'vQstr41': 'Default',
            'vQstr42': '',
            'vQstr43': 'DATE_IN',
            'vQstr44': f'Default,DATE_IN,>=,{filter_date},,,,,DATE_IN',
        }

        # defaults to `self.parse` callback
        yield FormRequest(url=self.start_urls[0], formdata=formdata)

    def parse(self, response):
        """Parse response obtained with form request.

        Response contains a table of portcalls that we need to extract.

        Args:
            scrapy.Response:

        Yields:
            Dict[str, str]:

        """
        # memoise reported_date so it won't need to be called repeatedly
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        )

        for idx, row in enumerate(response.xpath('//span//table//tr')):
            # header will always be the first row in the table
            if idx == 0:
                header = [
                    may_strip(head) for head in row.xpath('.//text()').extract() if may_strip(head)
                ]
                continue

            # retrieve flattend list of table row containing portcall info
            data = parser.flatten_table_row(row.xpath('.//span'))
            raw_item = {head: data[cell_idx] for cell_idx, head in enumerate(header)}
            # contextualise raw item with meta info
            raw_item.update(
                port_name=self.name, provider_name=self.provider, reported_date=reported_date
            )
            yield normalize.process_item(raw_item)
