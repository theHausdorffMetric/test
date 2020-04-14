import xlrd

from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.daily_howe_rob import normalize


class DailyHoweRobSpider(CharterSpider, MailSpider):
    name = 'HR_Daily_CPP_OIL'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]
    provider = 'Howe Rob'
    version = '1.0.0'

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
            for sheet in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets():
                for idx, raw_row in enumerate(sheet.get_rows()):
                    row = format_cells_in_row(raw_row, sheet.book.datemode)

                    # ignore irrelevant rows
                    if idx == 0:
                        continue

                    # extract header row, rpeort headers are inconsistent
                    if 'date reported' in row[0].lower():
                        header = row
                        continue

                    raw_item = {head.lower(): row[idx] for idx, head in enumerate(header)}
                    raw_item.update(provider_name=self.provider)
                    yield normalize.process_item(raw_item)

    @property
    def missing_rows(self):
        """So that analysts will be notified."""
        return normalize.MISSING_ROWS
