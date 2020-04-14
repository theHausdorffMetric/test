from dateutil.parser import parse as parse_date
import xlrd

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.os_greece import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


class OSGreeceSpider(ShipAgentMixin, MailSpider):
    name = 'OS_GreeceGrades_CPP'
    provider = 'Ocean Shipbrokers'
    version = '1.0.0'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    reported_date = None
    sheet_index = [0, 2, 3, 6]

    def parse_mail(self, mail):
        """Entry point of OS_GreeceGrades_CPP spider.

        Args:
            mail (Mail):

        Returns:
            PortCall:

        """
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        for attachment in mail.attachments():
            if not attachment.is_spreadsheet:
                continue

            for idx in self.sheet_index:
                sheet = xlrd.open_workbook(
                    file_contents=attachment.body, on_demand=True
                ).sheet_by_index(idx)

                header = None
                for idx, xlrd_row in enumerate(sheet.get_rows()):
                    row = [
                        may_strip(cell.value) if isinstance(cell.value, str) else cell.value
                        for cell in xlrd_row
                    ]
                    if idx == 1:
                        header = row
                        continue

                    if header:
                        raw_item = {h: row[idx] for idx, h in enumerate(header)}
                        raw_item.update(self.meta_field)

                        yield from normalize.process_item(raw_item)
                    else:
                        self.logger.warning('No headers are found.')

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
