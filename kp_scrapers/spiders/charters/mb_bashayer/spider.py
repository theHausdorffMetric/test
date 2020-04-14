from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.mb_bashayer import normalize


START_PROCESSING_SIGN = '------'
END_PROCESSING_SIGN = 'CARGO SPECS'
COMPLETE_ROW_LENGTH = 7


class MbBashairSpider(CharterSpider, MailSpider):
    name = 'MB_BashairFixtures_OIL'
    provider = 'Maven Brokers'
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
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')
        body = self.select_body_html(mail)

        processing_started = False
        for row_sel in body.xpath('//p'):
            raw_row = ''.join(row_sel.xpath('.//text()').extract())
            row = [may_strip(cell) for cell in raw_row.split('\xa0') if may_strip(cell)]

            if START_PROCESSING_SIGN in row:
                processing_started = True
                continue

            if END_PROCESSING_SIGN in row:
                return

            # discard items that are not required
            if len(row) != COMPLETE_ROW_LENGTH:
                continue

            if processing_started and row:
                raw_item = {str(idx): cell for idx, cell in enumerate(row)}
                raw_item.update(self.meta_field)
                self.logger.info(raw_item)

                yield normalize.process_item(raw_item)

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
