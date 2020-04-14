import xlrd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.inter_siot import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


class InteradriaGradesSpider(ShipAgentMixin, MailSpider):
    name = 'IN_Siot_Grades'
    provider = 'Interadria'
    version = '1.2.2'
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
                # report seperates date and timestamp into 2 cells,
                # this would cause the cell to try to convert the timestamps into a date
                # hence throwing an error (index affected 2,5,9)
                row = [
                    xldate_to_datetime(cell.value, sheet.book.datemode).isoformat()
                    if is_xldate(cell) and cell_idx not in (2, 5, 9)
                    else cell.value
                    for cell_idx, cell in enumerate(row)
                ]

                if idx == 0:
                    header = row
                    continue

                if header:
                    raw_item = {h: row[idx] for idx, h in enumerate(header)}
                    raw_item.update(provider_name='Interadria', reported_date=self.reported_date)

                    yield normalize.process_item(raw_item)
                else:
                    self.logger.warning('No headers are found.')
