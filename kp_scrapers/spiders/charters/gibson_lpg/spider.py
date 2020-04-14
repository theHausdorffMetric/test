from dateutil.parser import parse as parse_date

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.gibson_lpg import normalize, parser


class GibsonLpgSpider(CharterSpider, PdfSpider, MailSpider):
    name = 'GB_EA_Fixtures_LPG'
    provider = 'Gibson'
    version = '1.1.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel, DataTypes.Cargo]

    tabula_options = {
        '--guess': [],  # guess the portion of the page to analyze per page
        '--stream': [],  # force PDF to be extracted using text stream-mode
        '--pages': ['all'],  # extract from all pages
    }

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    reported_date = None

    def parse_mail(self, mail):
        """Entry point of GB_EA_LPG spider for running fixtures and grades.

        Args:
            mail (Mail):

        Yields:
            SpotCharter:

        """
        # memoise reported date so we don't need to call it later on
        reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        for attachment in mail.attachments():
            if attachment.is_pdf and 'gibson' in attachment.name.lower():
                table = self.extract_pdf_io(attachment.body, **self.tabula_options)
                for raw_item in parser.process_table(table):
                    # contextualise raw item with meta info
                    raw_item.update(provider_name=self.provider, reported_date=reported_date)
                    yield normalize.process_item(raw_item)
