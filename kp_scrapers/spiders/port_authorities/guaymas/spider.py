import datetime as dt

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.guaymas import normalize


def _remove_empty_strings(table):
    """Remove empty strings from pdf rows.

    Args:
        table (List[List[str | unicode]]): list of table rows from pdf

    Yields:
        List[str | unicode]:

    """
    for row in table:
        yield [element for element in row if element]


class GuaymasSpider(PortAuthoritySpider, PdfSpider):
    name = 'Guaymas'
    provider = 'Guaymas'
    version = '1.2.3'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['https://www.puertodeguaymas.com.mx/descargas/arribos.pdf']

    tabula_options = {
        '--lattice': []  # lattice-mode extraction (more reliable when pdf table has cell borders)
    }

    def parse(self, response):
        """Dispatch response to corresponding parsing function depending on URL.

        PDF, when extracted, looks like the following:  # noqa
            - `reported_date` row: ['FOLIO No. 037', '11 DE SEPTIEMBRE DEL 2018']
            - `headers` row:       ['BUQUE NACIONAL/ESLORA/TBR', 'PROCEDENTE DE\nE.T.A.', 'C  A   L  A   D  OS\nTRAMO', 'TRAFICO', 'TIPO DE MANIOBRA', 'TONELAJE     PRODUCTO', 'PROCEDENCIA/ DESTINO\nDE LA CARGA', 'EMBARCADOR\n/RECIBIDOR', 'AGENCIA\nCONSIGNATARIA', 'DESTINO FECHA DE SALIDA']
            - `data` row #1:       ['SAKURA DREAM\n180M/23 264MT', 'FONDEADO', '6.8\nT-4', 'ALTURA', 'CARGA\nPATIO A BUQUE', '33,000 MT\nCOBRE', 'MX', 'SEISA', 'MACOVE', 'CHINA\nTBC']
            - `data` row #2:       ['INTERLINK AFFINITY\n180M/24,940 MT', 'FONDEADO', '10.65\nT-4', 'ALTURA', 'CARGA\nPATIO A BUQUE', '26 125 MT CU CONC\n10 000 MT COPPER REVERTS', 'MX', 'SEISA', 'MACOVE', 'ESPAÑA\nTBC']
            - `data` row #3:       ['FANFARE\n138.06/ 9611 TON', 'FONDEADO', '7.8\nT-3', 'CABOTAJE', 'CARGA\nPATIO A BUQUE', '6 300 MT\nMATA COBRISA', 'MX', 'SEISA', 'MACOVE', 'MANZANILLO\nTBC']
            - `data` row #4:       etc...

        Args:
            response (scrapy.HtmlResponse):

        Yields:
            Dict[str, str]:

        """
        # memoise reported date so it won't need to called repeatedly later
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        )

        table = self.extract_pdf_io(response.body, _remove_empty_strings, **self.tabula_options)
        headers = self._parse_headers(table[1])

        for row in table[2:]:
            # transform row to a dict type, with headers as keys, and cells as values
            if not row:
                continue

            raw_item = {headers[idx]: cell for idx, cell in enumerate(row)}
            # enrich raw item with meta info
            raw_item.update(
                port_name=self.name, provider_name=self.provider, reported_date=reported_date
            )
            yield normalize.process_item(raw_item)

    def _parse_headers(self, headers):
        """Clean header names to be more consistent.

        Due to the vagaries of pdf parsing, sometimes we end up with additional spaces
        in header names, so this function removes the extraneous spaces for consistency.

        Args:
            headers (List[str]):

        Returns:
            List[str]:
        """
        return [cell.replace(' ', '').replace('\n', '') for cell in headers]
