import xlrd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.benline_taiwan import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


HEADERS = [
    'country',
    'location',
    'vessel',
    'facility/terminal',
    'cargo',
    'volume/mt',
    'eta',
    'load/discharge',
    'receiver',
    'origin',
]


class BenLineTawiwanSpider(ShipAgentMixin, MailSpider):
    name = 'BL_Taiwan_Grades'
    provider = 'Ben Line'
    version = '1.1.0'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """parse 2 email attachments iron and coal for australia
        Args:
            mail (Mail):
        Yields:
            Dict[str, str]:
        """
        # memoise reported date so it does not need to be called repeatedly later
        self.reported_date = to_isoformat(mail.envelope['date'])

        for attachment in mail.attachments():
            if 'COAL' in attachment.name:
                sheet = xlrd.open_workbook(
                    file_contents=attachment.body, on_demand=True
                ).sheet_by_index(0)

                start_processing = None
                for idx, raw_row in enumerate(sheet.get_rows()):
                    row = format_cells_in_row(raw_row, sheet.book.datemode)

                    # detect if cell is a port cell and memoise it
                    if 'Country' in row[0]:
                        start_processing = True
                        continue

                    if start_processing:
                        raw_item = {h: row[idx] for idx, h in enumerate(HEADERS)}
                        raw_item.update(
                            provider_name=self.provider, reported_date=self.reported_date
                        )
                        yield normalize.process_item(raw_item)
