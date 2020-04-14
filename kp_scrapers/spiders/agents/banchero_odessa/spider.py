import xlrd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.banchero_odessa import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


class BancheroOdessaGradesSpider(ShipAgentMixin, MailSpider):
    name = 'BCR_Odessa_Grades'
    provider = 'Banchero'
    version = '1.0.0'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """This method will be called for every mail the search_term matched.

        Each vessel movement has an associated uuid that is linked to a cargo's uuid,
        allowing for easy retrieval of a vessel's cargo movement.

        However, each vessel may contain multiple cargo movements with the same uuid,
        therefore we store the products as a list value against the uuid key.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:
        """
        # memoise reported date so it does not need to be called repeatedly later
        self.reported_date = to_isoformat(mail.envelope['date'])

        for attachment in mail.attachments():
            # sanity check in case file is not a spreadsheet
            if not attachment.is_spreadsheet:
                continue

            sheet = xlrd.open_workbook(
                file_contents=attachment.body, on_demand=True
            ).sheet_by_index(0)

            header = None

            for idx, row in enumerate(sheet.get_rows()):
                row = [
                    xldate_to_datetime(cell.value, sheet.book.datemode).isoformat()
                    if is_xldate(cell)
                    else cell.value
                    for cell_idx, cell in enumerate(row)
                ]

                if 'approach' in row[3]:
                    # sheet has 2 tables with different headers,
                    # first table has header split into 2
                    header = row
                    header[2] = 'Vessel'
                    header[5] = 'Cargo'
                    continue

                if 'Status' in row[1]:
                    header = row
                    continue

                if header and len(header) == len(row):
                    raw_item = {h: row[idx] for idx, h in enumerate(header)}
                    raw_item.update(provider_name=self.provider, reported_date=self.reported_date)
                    yield normalize.process_item(raw_item)
