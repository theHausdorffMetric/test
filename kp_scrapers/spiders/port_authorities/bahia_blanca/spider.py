from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.bahia_blanca import normalize, parser


class BahiaBlancaSpider(PortAuthoritySpider, PdfSpider):
    name = 'BahiaBlanca'
    provider = 'BahiaBlanca'
    version = '2.0.1'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['https://puertobahiablanca.com/situacion_operativa/posicion.pdf']

    def parse(self, response):
        """Extract data from PDF report from source website.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        yield from self.extract_from_vessels_forecast(response)
        yield from self.extract_from_vessels_harbour(response)

    def extract_from_vessels_forecast(self, response):
        """Extract forecasted portcalls from report.

        Examples of expected rows yielded:  # noqa
            ['09/01/2019 CHIPOL CHANGJIANG Hong Kong 188', 'MR', 'D 1.500 MAT. PROYECTO ÉOLICO', 'CHINA', 'CHINA 5 GALVAN']
            ['09/01/2019 FERNI H Liberia 145', 'SW', 'D/C  13000/10000 NAFTA/GAS-OIL', 'ARGENTINA', 'REMOVIDO POSTA 1-2 72 HS']
            ['11/01/2019 DESERT EAGLE Grecia 225', 'SW', 'C 18.000 CEBADA', 'ARGENTINA', 'ARABIA SAUDITA TBB 9']
            ['11/01/2019 NORDIC HONG KONG Liberia 228', 'MR', 'D/C  200/200 CONTENEDORES', 'ARGENTINA', 'BRASIL SITIO 21']

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        for row in self.extract_pdf_io(response.body, **parser.PARSING_OPTIONS_FORECAST_TABLE):
            # extract reported date from table row
            if 'ANUNCIOS' in row[0]:
                reported_date = row[0]
                continue

            # only extract relevant data rows
            if row[0] and row[0][0].isdigit():
                raw_item = {str(idx): ele for idx, ele in enumerate(parser.parse_forecast_row(row))}
                # contextualise raw item with meta info
                raw_item.update(
                    port_name='Bahia Blanca',
                    provider_name=self.provider,
                    # crash if reported_date not present, since it is a mandatory field
                    reported_date=reported_date,
                )
                yield normalize.process_forecast_item(raw_item)

    def extract_from_vessels_harbour(self, response):
        """Extract portcalls of vessels in harbour from report.

        Examples of expected rows yielded:  # noqa
            ['', 'ADM', 'ZHENG HENG', 'Panamá', '229', 'ISA', 'C19.425', 'MAIZ', 'VIETNAM', '', '15/04/18', '13:32', '', '']
            ['', 'Mega', 'EXEMPLAR', 'Bélgica', '291', 'SW', '', '', '', '', '04/12/17', '17:19', '', 'Regasificador']
            ['', '5 Galván', 'GENCO PREDATOR', 'Islas Marshall', '190', 'SW', 'D22.361', 'FERTILIZANTES', 'IMPORTACION', '', '15/04/18', '19:20', '', '']
            ['', 'Punta Cigüeña', 'CABO SOUNION', 'Islas Marshall', '221', 'AU', 'D65.000', 'CRUDO', 'REMOVIDO', '', '15/04/18', '19:20', '', '']

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        is_relevant_row = False
        for row in self.extract_pdf_io(response.body, **parser.PARSING_OPTIONS_HARBOUR_TABLE):
            # extract reported date from table row
            if 'Movimiento(ton)' in row[0].replace(' ', ''):
                reported_date = row[0]
                continue

            # actual portcall data starts right after this row
            if 'SITIOBUQUE' in ''.join(row):
                is_relevant_row = True
                continue

            # # actual portcall data ends right after this row
            if 'BUQUES EN FONDEADERO' in ''.join(row):
                is_relevant_row = False
                continue

            # do not process irrelevant rows
            # do not process relevant rows without vessels in them
            # vessel name will always be listed in the third table column
            if not is_relevant_row or not (len(row) > 2 and row[2]):
                continue

            # extract relevant row now; each row corresponds to a single berth
            raw_item = {str(idx): ele for idx, ele in enumerate(row)}
            # contextualise raw item with meta info
            raw_item.update(
                port_name='Bahia Blanca',
                provider_name=self.provider,
                # crash if reported_date not present, since it is a mandatory field
                reported_date=reported_date,
            )
            yield normalize.process_harbour_item(raw_item)
