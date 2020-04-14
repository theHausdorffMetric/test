from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.dar_es_salam import normalize, parser


class DarEsSalamSpider(PortAuthoritySpider, PdfSpider):
    name = 'DarEsSalam'
    provider = 'Tanzania'
    version = '1.1.0'

    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['https://ports.go.tz/index.php/en/shipping']

    port_name = 'Dar Es Salam'
    reported_date = None
    tabula_options = {
        '--guess': [],  # guess the portion of the page to analyze per page
        '--lattice': [],  # force PDF to be extracted using lattice-mode extraction
    }

    def parse(self, response):
        self.reported_date = parser.get_reported_date(response)

        # reports will always come in pairs of EXPECTED/BERTHED
        # however, the order they arrive in is not certain
        urls = response.xpath('//a[@class="rs_modal hasTooltip"]/@href').extract()
        for url in urls[:2]:
            # expected arrival
            if 'EXPECTED' in url:
                yield response.follow(url, self.parse_expected_arrival)
            # berth plan
            if 'BERTH' in url:
                yield response.follow(url, self.parse_berth_plan)

    def parse_expected_arrival(self, response):
        """Process items in expected arrival pdf file.

        Args:
            response (scrapy.Response):

        Yields:
            PortCall:

        """
        table = self.extract_pdf_table(
            response,
            lambda x: parser.filter_rows_for_expected_arrival(x, parser.VESSEL_NAME_COL_IDX),
            **self.tabula_options,
        )

        eta_date = ''
        for row in table:
            raw_item = parser.map_row_to_dict_for_expected_arrival(row, **self.meta_fields)

            # if the first column eta date is empty, need to refer to previous eta date
            eta_date = raw_item['0'] if raw_item['0'] else eta_date
            raw_item['0'] = eta_date

            yield normalize.process_eta_item(raw_item)

    def parse_berth_plan(self, response):
        """Parse berth plan pdf file.

        There are two types of table could be extracted:
        1. at berth
        2. anchorage

        Args:
            response (scrapy.Response):

        Returns:
            PortCall:

        """
        raw_table = self.extract_pdf_table(response, lambda x: x, **self.tabula_options)

        # ships at berth
        at_berth_table = list(parser.extract_at_berth_table(raw_table))
        at_berth_headers = [
            'berth',
            'vessel_name',
            'berth_draught',
            'berth_length',
            'ship_draught',
            'ship_length',
            'sailing_draught',
            'import',
            'export',
            'cargo',
        ]

        for row in at_berth_table:
            raw_item = {head: row[idx] for idx, head in enumerate(at_berth_headers) if head}
            raw_item.update(self.meta_fields)

            yield normalize.process_at_berth_item(raw_item)

        # ships at anchorage
        anchorage = parser.extract_anchorage_table(raw_table)
        anchorage_headers = parser.validate_anchorage_headers(anchorage[0])

        for row in anchorage[1:]:
            raw_item = {head: row[idx] for idx, head in enumerate(anchorage_headers) if head}
            raw_item.update(self.meta_fields)

            yield normalize.process_anchorage_item(raw_item)

    @property
    def meta_fields(self):
        return {
            'port_name': self.port_name,
            'provider_name': self.provider,
            'reported_date': self.reported_date,
        }
