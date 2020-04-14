from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.optima_tankers import normalize, parser


START_PROCESSING_WORD = ['VESSEL', 'BSEA/MED/UKC/BALTIC']
STOP_PROCESSING_WORD = ['PERIOD', 'T/C MARKET']


class OptimaTankersSpider(CharterSpider, MailSpider):
    name = 'OT_Fixtures'
    provider = 'Optima Tankers'
    version = '1.0.1'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    reported_date = None

    def parse_mail(self, mail):
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')
        self.mail_title = mail.envelope['subject']
        body = self.select_body_html(mail)
        start_processing = False
        for raw_row in body.xpath('//text()').extract():
            raw_row = may_strip(raw_row)
            if any(sub in raw_row for sub in START_PROCESSING_WORD):
                start_processing = True

            if any(sub in raw_row for sub in STOP_PROCESSING_WORD):
                break

            if start_processing:
                row, headers = parser.split_row(raw_row)
                if row:
                    raw_item = {headers[idx]: cell for idx, cell in enumerate(row)}
                    raw_item.update(
                        {
                            'provider_name': self.provider,
                            'reported_date': self.reported_date,
                            'mail_title': self.mail_title,
                        }
                    )
                    yield normalize.process_item(raw_item)

    @property
    def missing_rows(self):
        return parser.MISSING_ROWS + normalize.MISSING_ROWS
