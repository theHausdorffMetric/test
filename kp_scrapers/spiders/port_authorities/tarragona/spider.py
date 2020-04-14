import json

from scrapy import FormRequest, Request, Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.tarragona import normalize


class TarragonaSpider(PortAuthoritySpider, Spider):
    name = 'Tarragona'
    provider = 'Tarragona'
    version = '2.0.2'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        # login
        'http://tarragona.posidoniaport.com/gisweb/login.do?metodo=login',
        # last update time (main page)
        'http://tarragona.posidoniaport.com/gisweb/lastupdatetime.do?metodo=generarPantalla',
        # vessels list (main page)
        'http://tarragona.posidoniaport.com/gisweb/atraqueop.do?metodo=list',
        # vessel detail post request
        'http://tarragona.posidoniaport.com/gisweb/atraque.do?metodo=generarPantalla',
    ]

    reported_date = None

    def start_requests(self):
        """Simulate login action by sending post request to get the cookies.

        Args:
            response (Response):

        Yields:
            FormRequest:

        """
        yield FormRequest(
            url=self.start_urls[0], formdata={'autpor': '71'}, callback=self.parse_main_page
        )

    def parse_main_page(self, response):
        """Get reported date and vessel list from main page.

        Args:
            response (Response):

        Yields:
            Request:

        """
        # parse reported date
        yield Request(url=self.start_urls[1], callback=self.parse_reported_date)

        # parse vessels list
        yield Request(url=self.start_urls[2], callback=self.post_vessel_detail)

    def post_vessel_detail(self, response):
        """Send post request to get vessel detail.

        Args:
            response (Response):

        Yields:
            FormRequest

        """
        data = json.loads(response.body)['rows']

        for i in data:
            row = i['cell']

            # the value is string format, so use json.dumps
            formdata = {
                'pk': json.dumps(
                    {
                        "autpor": row[1],
                        "codpue": row[2],
                        "escanyo": row[3],
                        "codatr": row[4],
                        "esccod": row[5],
                        "esccodpue": row[6],
                    }
                )
            }

            yield FormRequest(
                url=self.start_urls[3], formdata=formdata, callback=self.parse_vessel_detail
            )

    def parse_vessel_detail(self, response):
        """Transform vessel with detailed info into usable events.

        Args:
            response (Response):

        Yields:
            Dict[str, str]

        """
        raw_item = json.loads(response.body)
        raw_item.update(self.meta_fields)

        yield normalize.process_item(raw_item)

    def parse_reported_date(self, response):
        """Get reported date from scrapy Response.

        Args:
            response (Response):

        Returns:
            None

        """
        self.reported_date = json.loads(response.body)['fecactualizacion']

    @property
    def meta_fields(self):
        return {
            'port_name': self.name,
            'provider_name': self.provider,
            'reported_date': self.reported_date,
        }
