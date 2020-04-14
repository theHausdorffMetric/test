import datetime as dt

from scrapy import FormRequest, Spider

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.marseille import normalize, parser


# describes vessel operation to filter by; listed exhaustively here
# NOTE discussed with analysts, we only want scheduled and future portcalls after all
STATUS_TYPES = ['PREVUE', 'PROGRAMMEE']  # expected to arrive  # scheduled to berth

# describes type of vessel to be scraped
VESSEL_TYPES = [
    'GAZ LIQUEFIES',  # gas (lng/lpg) tanker
    'MINERALIER',  # ore carrier
    'PETROL/MINERALIER',  # chemical tanker
    'PETROLIER',  # oil tanker
    'VRAQUIER SOLIDE',  # bulk carrier
]


class MarseilleSpider(PortAuthoritySpider, Spider):
    """Marseille spider class.

    Website contains scheduled/expected portcalls for many vessel types,
    each of which can be in various statuses in regard to their operation
    (e.g. vessel is "scheduled", vessel is "expected", etc.)

    FIXME For as of yet unknown reasons, Marseille spider will not provide correctly
    matched IMO if we filter and scrape multiple vessel types and statuses at a single go.
    Likely related to the `viewstate` field.

    """

    name = 'Marseille'
    provider = 'Marseille'
    version = '2.1.1'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['https://pcs.marseille-port.fr/webix/public/escales/recherche']

    def __init__(self, status, vessel_type):
        super().__init__()
        self.status = status
        self.vessel_type = vessel_type

    def parse(self, response):
        """Parse and populate form elements within portcall search page.

        Args:
            response (scrapy.Response):

        Yields:
            FormRequest:

        """
        # obtain keys denoting search classification and viewstate for form request
        _source = response.xpath('//button[@class="btn btn btn-primary"]/@id').extract_first()
        _viewstate = response.xpath('//input[@name="javax.faces.ViewState"]/@value').extract_first()

        # abstract away underlying form functionality for a cleaner API
        form = parser.PortCallJsfForm(source=_source, viewstate=_viewstate)
        form.query(status=self.status, vessel_type=self.vessel_type)

        # A formrequest is made to search for a list of port calls.
        yield FormRequest(
            url=self.start_urls[0],
            formdata=form.asdict(),
            meta={'viewstate': form.viewstate},
            callback=self.parse_table,
        )

    def parse_table(self, response):
        """Parse table containing portcalls and request vessel details for each portcall.

        Args:
            response (scrapy.XmlResponse):

        Yields:
            FormRequest:

        """
        # scrapy autoselects XmlResponse, but resource is actually in HTML
        # force response type to HTML
        html = parser.html_response(response)

        # iterate through each table row (i.e. portcall) in the response
        for row in html.xpath('//tbody/tr'):
            # get `source` of current row, i.e. key that describes current search category
            _source = row.xpath('td[2]/a/@id').extract_first()
            # abstract away underlying form functionality for a cleaner API
            form = parser.PortCallJsfForm(source=_source, viewstate=response.meta['viewstate'])

            yield FormRequest(
                url=self.start_urls[0],
                formdata=form.asdict(),
                # cache `raw_item` to allow more fields to be appended to it in the callback
                meta={'raw_item': row_to_dict(row, header=html.xpath('//th/text()').extract())},
                callback=self.parse_vessel_details,
            )

    def parse_vessel_details(self, response):
        """Parse vessel details for specified portcall and enrich raw item with it.

        Args:
            response (scrapy.XmlResponse):

        Yields:
            Dict[str, Any]:

        """
        # scrapy autoselects XmlResponse, but resource is actually in HTML
        # force response type to HTML
        vessel = parser.html_response(response).xpath('//div[@class="form-group  col-md-12"]')

        # append `raw_item` with additional vessel details
        response.meta['raw_item'].update(
            dict(
                zip(
                    # field names
                    vessel.xpath('.//label/text()').extract(),
                    # field values
                    vessel.xpath('.//p/text()').extract(),
                )
            )
        )

        # append raw item with meta info
        response.meta['raw_item'].update(
            provider_name=self.provider,
            reported_date=dt.datetime.utcnow()
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .isoformat(),
        )

        yield normalize.process_item(response.meta['raw_item'])
