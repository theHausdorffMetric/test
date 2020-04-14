import datetime as dt

from scrapy import Request, Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.genoa import normalize, parser


BASE_URL = 'http://maildomino.portopetroli.com'


class GenoaSpider(PortAuthoritySpider, Spider):
    name = 'Genoa'
    provider = 'Genoa'
    version = '2.2.3'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://maildomino.portopetroli.com/portopetroli/Previsioni.nsf']

    def parse(self, response):
        """Dispatch response to corresponding callback given URL.

        Each vessel in the lineup has an individual page outlining info
        on its movement, its cargo and vessel identification details.

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            Dict[str, str]:

        """
        # memoise reported date
        self.reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
        )

        # extract detailed vessel movement for each vessel
        for url in self._get_vessel_movement_urls(response):
            yield Request(url=url, callback=self.parse_vessel_movement_url)

    def parse_vessel_movement_url(self, response):
        """Parse individual vessel movement for each vessel in the lineup in `start_url`.

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            ArrivedEvent:
            EtaEvent:

        """
        # extract port activity and vessel details
        port_vessel_table = parser.extract_activity_detail_rows(response)
        port_vessel_raw = parser.extract_raw_from_rows(
            port_vessel_table,
            cargoes=list(parser.extract_cargo_movement_rows(response)),
            port_name=self.name,
            provider_name=self.provider,
            reported_date=self.reported_date,
        )
        return normalize.process_item(port_vessel_raw)

    @staticmethod
    def _get_vessel_movement_urls(response):
        """Extract URLs of detailed vessel movement from table rows.

        Args:
            response (scrapy.Response):

        Returns:
            GeneratorType[List[str]]: individual vessel movement URLs

        """
        table = response.xpath('//table[@id="view:_id1:_id2:viewPanel1"]')
        relative_paths = table.xpath('.//td[@style="width:96.0px"]/a/@href').extract()
        return (BASE_URL + single_path for single_path in relative_paths)
