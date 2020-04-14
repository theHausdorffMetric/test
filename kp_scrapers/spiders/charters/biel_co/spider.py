import xlrd

from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.biel_co import normalize


# Header row
HEADER_ROW = 3


class BielCoGrades(CharterSpider, MailSpider):

    name = 'BNC_Petcoke_Grades'
    provider = 'B&C'
    version = '1.0.0'
    produces = [DataTypes.SpotCharter, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """Extract mail found with specified email filters in spider arguments.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:

        """
        for attachment in mail.attachments():
            # each xlsx file by this provider will only have one sheet
            sheet = xlrd.open_workbook(
                file_contents=attachment.body, on_demand=True
            ).sheet_by_index(0)

            self.port_name = mail.envelope['subject']

            for idx, raw_row in enumerate(sheet.get_rows()):
                row = format_cells_in_row(raw_row, sheet.book.datemode)

                # get reported date
                if idx == 1:
                    self.reported_date = row[12]

                # remove empty filler rows before and after the main data table
                # remove unnecessary rows
                if idx < HEADER_ROW or row[0] == 'TERMINAL POSTING':
                    continue

                # initialise headers
                if idx == HEADER_ROW:
                    header = row
                    continue

                raw_item = {head: row[idx] for idx, head in enumerate(header) if head}
                # contextualise raw item with metadata
                raw_item.update(
                    provider_name=self.provider,
                    reported_date=self.reported_date,
                    port_name=self.port_name,
                )
                yield normalize.process_item(raw_item)
