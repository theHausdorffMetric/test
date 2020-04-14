from dateutil.parser import parse as parse_date

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.mc_quilling import normalize


class McQuillingSpider(CharterSpider, PdfSpider, MailSpider):
    name = 'MQ_Fixtures_Dirty'
    provider = 'McQuilling'
    version = '1.0.0'
    produces = [DataTypes.SpotCharter]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    tabula_options = {'--stream': [], '--guess': []}

    reported_date = None

    def parse_mail(self, mail):
        """Parse pdf attachment.

        Args:
            mail (Mail):

        Returns:
            PortCall | Dict[str, str]

        """
        for attachment in mail.attachments():
            if not attachment.is_pdf:
                continue

            self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')
            table = self.extract_pdf_io(attachment.body, **self.tabula_options)

            start_processing = False
            for row in table:
                if 'Charterer' in row:
                    headers = row
                    start_processing = True
                    continue

                if start_processing:
                    filtered_row = [cell for cell in row if cell]
                    if len(filtered_row) != len(headers):
                        continue

                    raw_item = {header: row[idx] for idx, header in enumerate(headers)}
                    raw_item.update(**self.meta_field)
                    yield normalize.process_item(raw_item)

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
