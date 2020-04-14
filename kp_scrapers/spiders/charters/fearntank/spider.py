from dateutil.parser import parse as parse_date

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.fearntank import normalize, parser


class FearntankSpider(CharterSpider, MailSpider):
    name = 'Fearntank'
    provider = 'Fearntank'
    version = '1.0.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel, DataTypes.Cargo]

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
        for raw_row in body.xpath('//text()').extract():
            row, headers = parser.split_row(raw_row)
            if row:
                raw_item = {headers[idx]: cell for idx, cell in enumerate(row)}
                raw_item.update(self.meta_field)

                yield normalize.process_item(raw_item)

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
