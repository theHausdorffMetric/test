from dateutil.parser import parse as pd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import map_row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.ht_daily.parser import extract_date, parse_date, parse_quantity
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider


MISSING_ROWS = []


# Order of fields in the pdf table
ROW_HEADERS = ['port', 'status', 'vessel_name', 'date', 'movement', 'product', 'volume', 'trip']


MOVEMENT_MAPPING = {'UNLOAD': 'discharge', 'LOAD': 'load'}


PORT_MAPPING = {
    'FOS/LAVERA': 'Fos',
    'PORT JEROME': 'Exxon Port Jerome',
    'LA PALLICE': 'La Rochelle',
    'PORT LA NOUVELLE': 'Port-La-Nouvelle',
}


PDF_NAME = 'daily update'


class HTDailySpider(ShipAgentMixin, PdfSpider, MailSpider):
    name = 'HT_DailyUpdate'
    provider = 'Humann & Taconet'
    version = '0.1.0'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    tabula_options = {'--lattice': [], '--pages': ['all']}

    def parse_mail(self, mail):
        """The method will be called for every mail the search_term matched.
        """
        for attachment in mail.attachments():
            if PDF_NAME not in attachment.name.lower() or not attachment.is_pdf:
                continue
            return self.parse_pdf(attachment.body, attachment.name)

    def parse_pdf(self, body, name):
        """Parse PDF reports.

        PDF is structured such that extracted data will look something like this (# noqa):
        0 ['PORT', 'STATUS', 'VESSEL', 'BERTH', 'OPERATION', 'PRODUCT', 'TONNAGE', 'FROM / TO']
        1 ['PORT JEROME', 'ALONGSIDE', 'MAERSK ROSYTH', 'PJG3', 'UNLOAD', 'HYDROCRACKAT', '26 000', 'ROTTERDAM(NL)']
        2 ['', '', '', '', '', '', '', '']
        3 ['LE HAVRE', 'ALONGSIDE', 'MAERSK PROGRESS', 'CIM', 'UNLOAD', 'ULSD', '95 000', 'YANBU / ROTTERDAM']
        4 ['', '', '', '', '', '', '', '']
        5 ['DONGES', 'ALONGSIDE', 'CIELO DI HANOI', 'ARCEAU', 'UNLOAD', 'ULSD', '30 000', 'PRIMORSK']
        6 ['DONGES', 'ALONGSIDE', 'RAMIRA', 'DGS5', 'LOAD', 'NAPHTA', '14 000', '']
        7 ['', '', '', '', '', '', '', '']
        8 ['BORDEAUX', 'ALONGSIDE', 'STEN FRIGG', '511 AMBES', 'UNLOAD', 'JET FUEL', '10 000', 'LE HAVRE']
        9 ['BORDEAUX', 'ALONGSIDE', 'INYALA', '511 AMBES', 'UNLOAD', 'GO', '30 000', 'ANVERS']
        10 ['', '', '', '', '', '', '', '']
        11 ['FOS/LAVERA', 'ALONGSIDE', 'FS CLARA', '0B', 'LOAD', 'GASOLINE LS', '1 050', 'BASTIA']
        12 ['FOS/LAVERA', 'ALONGSIDE', 'FS CLARA', '0B', 'LOAD', 'ULSD', '3 450', 'BASTIA']
        13 ['FOS/LAVERA', 'ALONGSIDE', 'MAERSK MISUMI', 'C', 'LOAD', 'NAPHTA', '5 000', 'JEBEL ALI']
        14 ['FOS/LAVERA', 'ALONGSIDE', 'MAERSK MISUMI', 'C', 'LOAD', 'GASOLINE HS', '22 350', 'JEBEL ALI']
        15 ['PORT', 'STATUS', 'VESSEL', 'ETA', 'OPERATION', 'PRODUCT', 'TONNAGE', 'FROM / TO']
        16 ['', '', '', '', '', '', '', '']
        17 ['DUNKIRK', 'EXPECTED', 'MRC HATICE ANA', '05/06/2018', 'LOAD', 'C6', '5 500', 'PORTO MARGHERA']
        18 ['', '', '', '', '', '', '', '']
        19 ['PORT JEROME', 'EXPECTED', 'STENSTRAUM', '05/06/2018', 'UNLOAD', 'NAPHTA', '7 700', 'ANTWERP']
        20 ['', '', '', '', '', '', '', '']
        21 ['LE HAVRE', 'EXPECTED', 'STI CONDOTTI', '05/06/2018', 'UNLOAD', 'JET FUEL', '93 000', 'MINA AL AHMADI / ROTTERDAM']
        22 ['LE HAVRE', 'EXPECTED', 'LIV KNUTSEN', '05/06/2018', 'UNLOAD', 'ALKYLATE', '7 000', 'ROTTERDAM']
        23 ['LE HAVRE', 'EXPECTED', 'SPECIALITY', '06/06/2018', 'UNLOAD', 'NAPHTA', '2 000', 'DONGES / ROTTERDAM']
        24 ['', '', '', '', '', '', '', '']
        25 ['BREST', 'EXPECTED', 'SELANDIA SWAN', '13/06/2018', 'UNLOAD', 'GO', '11 000', 'COPENHAGUE']
        26 ['', '', '', '', '', '', '', '']
        27 ['DONGES', 'EXPECTED', 'LIV KNUTSEN', '02/06/2018', 'UNLOAD', 'GASOLINE', '6 700', '']
        28 ['DONGES', 'EXPECTED', 'ANGLEVIKEN', '30/05/2018', 'LOAD', 'GASOLINE', '8 840', '']
        29 ['DONGES', 'EXPECTED', 'SARNIA CHERIE', '30/05/2018', 'LOAD', 'GO', '3 200', 'DOUARNENEZ']
        30 ['', '', '', '', '', '', '', '']
        31 ['LA PALLICE', 'EXPECTED', 'ANGLEVIKEN', '04/06/18', 'UNLOAD', 'GASOLINE', '9 280', 'DONGES']
        32 ['', '', '', '', '', '', '', '']
        33 ['BORDEAUX', 'EXPECTED', 'BALTIC WIND', '04/06/2018', 'UNLOAD', 'GO', '30 000', 'BROFJORDEN']
        34 ['BAYONNE', 'EXPECTED', 'PATALYA', '05/06/2018', 'UNLOAD', 'GO', '15 000', 'AMSTERDAM']
        35 ['', '', '', '', '', '', '', '']
        36 ['PORT LA NOUVELLE', 'EXPECTED', 'VALLE DI NAVARRA', '05/06/2018', 'UNLOAD', 'GO', '29 400', 'MARSAXLOKK']
        37 ['', '', '', '', '', '', '', '']
        38 ['FOS/LAVERA', 'EXPECTED', 'ATRIA', '05/06/2018', 'UNLOAD', 'GASOLINE LS', '7 500', '']
        39 ['FOS/LAVERA', 'EXPECTED', 'ATRIA', '05/06/2018', 'UNLOAD', 'LIGHT VIRGIN NAPHTA', '2 700', '']
        40 ['FOS/LAVERA', 'EXPECTED', 'ATRIA', '05/06/2018', 'UNLOAD', 'ALKYLATE', '3 000', '']
        41 ['FOS/LAVERA', 'EXPECTED', 'HISTRIA AGATA', '07/06/2018', 'UNLOAD', 'JET FUEL', '32 947', '']
        42 ['FOS/LAVERA', 'EXPECTED', 'ST SOLENE', 'ANCHORED', 'LOAD', 'GASOLINE LS', '4 600', 'PORT LA NOUVELLE']
        43 ['FOS/LAVERA', 'EXPECTED', 'BRITISH ALTUS', 'ANCHORED', 'UNLOAD', 'GASOLINE LS', '9 000', 'GIBRALTAR']

        Row 0 consists of column names.
        Row 1 to 43 are relevant rows of data with spaces in between that would be ignored.

        Args:
            attachment (Attachment): mail attachment object

        Yields
            Dict[str, str]:

        """
        # reported date is within the name of the pdf attachment ('Daily Update 25-5-2018.pdf')
        self.reported_date = extract_date(name, r'\d+-\d+-\d+')

        for idx, raw_row in enumerate(self.extract_pdf_io(body, **self.tabula_options)):
            # ignore all the header rows or empty rows (using first col, PORT field as proxy)
            if raw_row[0] in ['PORT', ''] or len(raw_row) < len(ROW_HEADERS):
                continue

            row = map_row_to_dict(raw_row, ROW_HEADERS)
            matching_date = parse_date(row['date'], self.reported_date)
            # dates can be wrongly keyed in i.e 20/0/2019, check if isdate
            # and append failed cases to missing rows
            if not matching_date or not self.is_date(matching_date):
                MISSING_ROWS.append(str(raw_row))
                continue

            yield self._build_item(row, matching_date)

    @validate_item(CargoMovement, normalize=True, strict=False)
    def _build_item(self, row, matching_date):
        return {
            'port_name': PORT_MAPPING.get(row['port'], row['port']),
            'reported_date': to_isoformat(self.reported_date),
            'eta': to_isoformat(matching_date),
            'provider_name': self.provider,
            'cargo': {
                'product': row['product'],
                'movement': MOVEMENT_MAPPING.get(row['movement']),
                'volume': parse_quantity(row['volume']),
                'volume_unit': Unit.tons,
            },
            'vessel': {'name': row['vessel_name']},
        }

    @staticmethod
    def is_date(date_string):
        """ Check if date can be parsed as date

        Args:
            date_string (str):

        Returns:
            Bool


        """
        try:
            pd(date_string)
            return True

        except ValueError:
            return False

    @property
    def missing_rows(self):
        return MISSING_ROWS
