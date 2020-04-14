from scrapy import Spider
from scrapy.http import FormRequest

from kp_scrapers.lib import static_data
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.barcelona import parser


class BarcelonaSpider(PortAuthoritySpider, Spider):
    name = 'Barcelona'
    provider = 'Barcelona'
    version = '1.1.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ('http://www.portdebarcelona.cat/ConsultaEscalas/?language=US',)

    def parse(self, response):
        """Parse and populate form elements for each vessel.

        Args:
            response (scrapy.Response):

        Yields:
            FormRequest:

        """

        # the source contains a huge list of vessel IMOS(6k~)
        # Inorder to reduce the number of request we make, vessel imo's from platform are obatined
        # and the request are made only to those imo's.
        imo_from_website = set(response.xpath('//select/option/@value').getall())
        imos_from_platform = set(
            [vessel['imo'] for vessel in static_data.vessels() if vessel['imo']]
        )
        imos_matched_with_platform = imo_from_website & imos_from_platform

        for imo in imos_matched_with_platform:
            formdata = {'regLloyds': imo, 'codbuq': imo}

            yield FormRequest.from_response(
                response, formdata=formdata, callback=self._parse_listing
            )

    def _parse_listing(self, response):
        """Parse and populate vessel info and port calls.

        Args:
            response (scrapy.Response)

        Yield:
            FormRequest

        """
        for item in parser.parse_listing_table(response):
            item.update({'provider_name': self.provider, 'port_name': self.provider})
            yield FormRequest(
                'http://www.portdebarcelona.cat/ConsultaEscalas/' 'detalleCrucero.jsp',
                formdata=item.pop('form_data'),
                callback=parser.parse_vessel_info,
                meta=item,
            )
