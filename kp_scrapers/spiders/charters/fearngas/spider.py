from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.fearngas import normalize


FIXTURES_START = ['LPG', 'PETCHEM']
FIXTURES_END = ['--', 'PERIOD']
MISSING_ROWS = []
ROW_LEN = 7


class FearngasSpider(CharterSpider, PdfSpider, MailSpider):
    name = 'FG_Fixtures_LPG'
    provider = 'Fearngas'
    version = '1.0.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    reported_date = None

    def parse_mail(self, mail):
        """Entry point of FG_Fixtures_LPG spider.

        Args:
            mail (Mail):

        Returns:
            SpotCharter:

        """
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')
        body = self.select_body_html(mail)
        do_process = False
        for raw_row in body.xpath('.//text()').extract():
            row = may_strip(raw_row).upper()

            if row in FIXTURES_START:
                do_process = True
                continue

            if row in FIXTURES_END:
                return

            if do_process:
                cells = row.replace('–', '-').split(' - ')
                if len(cells) != ROW_LEN:
                    MISSING_ROWS.append(row)
                else:
                    raw_item = {str(idx): cell for idx, cell in enumerate(cells)}
                    raw_item.update(self.meta_field)
                    yield normalize.process_item(raw_item)

    @property
    def missing_rows(self):
        """So that analysts will be notified."""
        return MISSING_ROWS

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
