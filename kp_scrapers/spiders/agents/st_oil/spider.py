import xlrd

from kp_scrapers.lib.excel import xldate_to_datetime
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.st_oil import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


RELEVANT_TABLES = ['SAILED', 'ALONGSIDE', 'AT THE ROADS', 'ON ARRIVAL']
REPORTED_DATE_CELL_COORDINATES = (1, 5)


class STOilSpider(ShipAgentMixin, MailSpider):
    name = 'ST_NovoOilTerminal'
    provider = 'Seatrade'
    version = '1.0.0'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]

    # port name provided by source
    port = 'Novorossiysk'

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """Parse each mail matched by the `query` spider argument.

        Data is structured as such in the XLS:

        ## BLOCK A
        ['', '', '', '', '', '', '', '', '', '']
        ["NOVOROSSIYSK 'SHESKHARIS' OIL TERMINAL REPORT", '', '', '', 'DATE', 43280.0, '', ...]
        ['', '', '', '', '', '', '', '', '', '']
        ['WEATHER FORECAST (valid till 29/21:00 lt):', '', '', '', '', '', '', '', '', '']
        ['WIND  : S 6-11 m/s, during thunderstorm gusts up to 12-15 m/s, occasional rain', '', ...]
        ['SEAS  : 0,8-1,3 m', '', '', '', '', '', '', '', '', '']
        ['AIR,T : +31;+33 C; ', '', '', '', '', '', '', '', '', '']
        ['', '', '', '', '', '', '', '', '', '']
        ['', '', '', '', '', '', '', '', '', '']
        ['STATUS OF TERMINAL:     ', '', 'O P E N ', ' FOR MOORING & LOADING OPS ', '', '', '', ...]

        ## BLOCK B
        ['SAILED', ' ', '', '', '', '', '', '', '', '']
        ['Vessel', 'Arrived', 'Ggo/Qtty', 'Berthed', 'Comm ldng', 'Compltd ldng', 'Sailed', ...]
        ['Alatau', '251800', 'CO/ 86381', '260721', '260855', '270930', '280050', 'Croatia', ...]
        ['Hope A', '240850', 'FO/ 36631', '252320', '260050', '271735', '280120', 'Israel', ...]
        ['Marathi', '250830', 'CO/145000', '271600', '271900', '281900', '290100', 'Romania', ...]
        ['Altai', '250100', 'CO/ 80000', '280400', '280530', '290200', '290800', 'Italy', ...]
        ['', '', '', '', '', '', '', '', '', '']

        Each sheet within the XLS will have BLOCK A as the sheet header, which contains
        irrelevant data we don't want.

        Following BLOCK #1, each sheet will contain multiple instances of BLOCK B,
        listed sequentially, with each block clearly demarcated with a row of
        ['', '', '', '', '', '', '', '', '', ''] as the delimiter bewtween blocks.

        BLOCK B can be identified by the presence of the vessel activity "SAILED", "ALONGSIDE",
        "AT THE ROADS" and "ON ARRIVAL", following which is the header of the block's table.
        Data within BLOCK B is to be extracted as it is relevant.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:

        """
        for attachment in mail.attachments():
            workbook = xlrd.open_workbook(file_contents=attachment.body, on_demand=True)
            for sheet in workbook.sheets():
                # memoise reported date so we don't have to call it for every item
                reported_date = self._extract_reported_date(sheet, *REPORTED_DATE_CELL_COORDINATES)

                # store states to identify rows of interest within sheet
                is_header_row = False
                is_data_row = False
                for row in sheet.get_rows():
                    row = [str(cell.value) for cell in row]

                    # optimise to discard irrelevant rows
                    # the end of each relevant table is also marked by an empty first cell
                    if not row[0] or row[0] == 'None':
                        is_header_row = False
                        is_data_row = False
                        continue

                    # check if we meet the criteria for finding relevant tables
                    if row[0] in RELEVANT_TABLES:
                        is_header_row = True
                        is_data_row = False
                        continue

                    # each relevant table always has a header, following which we have the data rows
                    if is_header_row:
                        header = [cell for cell in row if cell]
                        is_header_row = False
                        is_data_row = True
                        continue

                    # the core logic for extracting the data rows from the tables
                    if is_data_row:
                        raw_item = {head: row[idx] for idx, head in enumerate(header)}
                        # add some metadata to contexutalise raw data
                        raw_item.update(
                            port_name=self.port,
                            provider_name=self.provider,
                            reported_date=reported_date,
                        )
                        yield from normalize.process_item(raw_item)

    @staticmethod
    def _extract_reported_date(sheet, *coordinates):
        """Extract reported date listed inside sheet, as an ISO-8601 string.

        Args:
            sheet (xlrd.sheet):
            coordinates (int): (row, col) of cell where reported date is

        Returns:
            str: ISO-8601 formatted timestamp

        """
        return xldate_to_datetime(sheet.cell(*coordinates).value, sheet.book.datemode).isoformat()
