import json

from scrapy import FormRequest, Request, Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.cartagena import normalize


class CartagenaSpider(PortAuthoritySpider, Spider):
    name = 'Cartagena'
    provider = 'Cartagena'
    version = '2.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]
    reported_date = None

    start_urls = [
        # login
        'http://atraques.apc.es/gisweb/login.do?metodo=login',
        # last update time (main page)
        'http://atraques.apc.es/gisweb/lastupdatetime.do?metodo=generarPantalla',
        # vessels list (main page)
        'http://atraques.apc.es/gisweb/atraqueop.do?metodo=list',
        # vessel detail
        'http://atraques.apc.es/gisweb/atraque.do?metodo=generarPantalla',
    ]

    def start_requests(self):
        """Simulate login action by sending http post request to get the cookies.

        Args:

        Yields:
            scrapy.FormRequest:

        """
        yield FormRequest(
            url=self.start_urls[0], formdata={'autpor': '57'}, callback=self.parse_main_page
        )

    def parse_main_page(self, response):
        """Get reported date and vessel list.

        Actually this function sends two post request, for reported date and vessels list
        respectively.

        Args:
            response (scrapy.Response):

        Yields:
            scrapy.Request:

        """
        # parse reported date
        yield Request(url=self.start_urls[1], callback=self.parse_reported_date)

        # parse vessels list
        yield Request(url=self.start_urls[2], callback=self.post_vessel_detail)

    def post_vessel_detail(self, response):
        """Http post request with formdata to get a vessel's detailed info.

        Args:
            response (scrapy.Response):

        Yields:
            scrapy.FormRequest:

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
        """Already get a detailed vessel info, parse it.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        raw_item = json.loads(response.body)
        # contextualise raw item with meta info
        raw_item.update(
            port_name=self.name, provider_name=self.provider, reported_date=self.reported_date
        )

        yield normalize.process_item(raw_item)

    def parse_reported_date(self, response):
        """Parse reported date and assign it.

        Args:
            response (scrapy.Response):

        Returns:
            None:

        """
        self.reported_date = json.loads(response.body)['fecactualizacion']
