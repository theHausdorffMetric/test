from dateutil.parser import parse as parse_date

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.berge import normalize, parser


class BergeSpider(CharterSpider, PdfSpider, MailSpider):
    name = 'BR_Fixtures_LPG'
    provider = 'Bergé'
    version = '1.1.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
        'MARK_MAIL_AS_SEEN': True,
    }

    tabula_options = {'--guess': [], '--pages': ['all'], '--stream': []}

    reported_date = None
    missing_rows = []

    def parse_mail(self, mail):
        """Entry point of BR_Fixtures_LPG spider.

        Args:
            mail (Mail):

        Returns:
            SpotCharter:

        """
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        for attachment in mail.attachments():
            if attachment.is_pdf:
                table = self.extract_pdf_io(attachment.body, **self.tabula_options)
                for raw_item in parser.process_table(table):
                    raw_item.update(self.meta_field)

                    yield normalize.process_item(raw_item)

                # collect possible missing rows after processing all the items
                self.missing_rows = parser.MISSING_ROWS

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
