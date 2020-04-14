from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents.fbr_pakistan import normalize
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.contracts import ContractSpider
from kp_scrapers.spiders.contracts.bill_of_lading.spider import DEFAULT_NOT_TERMS


LAST_TEXT_NODE = './descendant::*/text()'
IRRELEVANT_DESTINATIONS = ['KAPW', 'KAPE', 'KPPI']
PDF_HEADERS = {'Goods Description', 'Shipment', 'Importer', '(M.Tons)'}


class FbrSpider(ContractSpider, PdfSpider):
    name = 'FBR_CargoMovements'
    version = '1.0.0'
    provider = 'Federal Board of Revenue'
    start_urls = ['http://o.fbr.gov.pk/newcu/igm/igm.asp']
    tabula_options = {'--pages': ['all'], '--guess': [], '--lattice': []}
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    @staticmethod
    def _process_rows(table_rows):
        for row in table_rows:
            if len(PDF_HEADERS.intersection(row)) != 0:
                continue
            else:
                yield row

    def parse(self, response):
        for table in response.xpath('//table'):
            for line in table.xpath('.//tr[position()>1]'):
                cols = line.xpath('./td')
                destination = cols[0].xpath(LAST_TEXT_NODE).get()
                url = cols[1].xpath('./a/@href').get()
                igm = cols[2].xpath(LAST_TEXT_NODE).get()
                vessel = cols[3].xpath(LAST_TEXT_NODE).get()
                arrival = cols[5].xpath(LAST_TEXT_NODE).get()
                berth = cols[6].xpath(LAST_TEXT_NODE).get()
                if url and destination not in IRRELEVANT_DESTINATIONS:
                    yield response.follow(
                        url=url,
                        callback=self.parse_pdf,
                        meta={
                            'port_name': destination,
                            'vessel': vessel,
                            'arrival': arrival,
                            'reported_date': igm,
                            'berth': berth,
                        },
                    )

    def parse_pdf(self, response):
        for row in self.extract_pdf_table(response, self._process_rows, **self.tabula_options):
            raw_item = {
                'port_name': response.meta['port_name'],
                'vessel': response.meta['vessel'],
                'arrival': response.meta['arrival'],
                'reported_date': response.meta['reported_date'],
                'berth': response.meta['berth'],
                'cargo_product': row[1],
                'cargo_volume': row[3],
                'cargo_unit': row[5],
                'cargo_buyer': row[7],
                'provider_name': self.provider,
            }
            yield normalize.process_item(raw_item, DEFAULT_NOT_TERMS)
