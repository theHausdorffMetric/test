from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.interocean import normalize


ROW_LEN = 7
MIN_ROW_LEN = 2
START_PROCESSING_SIGN = 'FIXTURES REPORTED'
STOP_PROCESSING_SIGN = 'FAILED'


class InteroceanSpider(CharterSpider, MailSpider):
    name = 'Interocean_Fixtures_OIL'
    provider = 'Interocean'
    version = '1.0.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    reported_date = None

    def parse_mail(self, mail):
        """Entry point of Interocean_Fixtures_OIL spider.

        Args:
            mail (Mail):

        Returns:
            PortCall:

        """
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        body = self.select_body_html(mail)

        processing_started = False
        for row_sel in body.xpath('//text()'):
            row = [may_strip(cell) for cell in row_sel.extract().split('\xa0') if may_strip(cell)]

            if START_PROCESSING_SIGN in row:
                processing_started = True
                continue

            if STOP_PROCESSING_SIGN in row:
                return

            if processing_started and row:
                # valid row
                if len(row) == ROW_LEN:
                    raw_item = {str(idx): cell for idx, cell in enumerate(row)}
                    raw_item.update(self.meta_field)

                    yield normalize.process_item(raw_item)

                # invalid row, log it
                elif len(row) > MIN_ROW_LEN:
                    raw_row = ' '.join(row)
                    self.logger.error(f'You might miss this row: {raw_row}')

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
