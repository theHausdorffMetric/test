import xlrd

from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.medco import normalize


class MecdoSpider(CharterSpider, MailSpider):
    name = 'MCD_Charter'
    provider = 'Medco'
    version = '1.0.0'

    produces = [DataTypes.SpotCharter, DataTypes.Vessel, DataTypes.Cargo]

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
            # some files have multiple sheets within them; extract all of them
            for sheet in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets():
                # only visible sheets are required
                if sheet.visibility == 0:
                    for raw_row in sheet.get_rows():
                        row = format_cells_in_row(raw_row, sheet.book.datemode)
                        # extract header row
                        if 'VESSEL' in row:
                            header = row
                            continue
                        # remove empty rows
                        if not (row[0] and row[1] and row[2]):
                            continue

                        raw_item = {head: row[idx] for idx, head in enumerate(header)}
                        raw_item.update(provider_name=self.provider,)
                        yield from normalize.process_item(raw_item)
