from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.alliance_panamax import normalize, parser


START_PHRASES = ['FIXTURES', 'CARGO']
STOP_PHRASES = ['THANK YOU.', 'ENQUIRIES']


class ATPanamaxMR(CharterSpider, MailSpider):
    name = 'AT_Fixtures'
    provider = 'Alliance Tankers'
    version = '1.0.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    reported_date = None

    def parse_mail(self, mail):
        """Parse the mail body and generate events.

        Args:
            mail (Mail):

        Returns:
            SpotCharter:

        """
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')
        body = self.select_body_html(mail)
        start_processing = False

        for row_html in body.xpath('//p'):
            raw_row = may_strip(''.join(row_html.xpath('.//text()').extract()))

            if any(sub in raw_row.upper() for sub in START_PHRASES):
                start_processing = True
                continue

            if any(sub in raw_row.upper() for sub in STOP_PHRASES):
                start_processing = False
                continue

            if start_processing:
                row, headers = parser.split_row(raw_row)
                if row:
                    raw_item = {headers[idx]: cell for idx, cell in enumerate(row)}
                    raw_item.update(reported_date=self.reported_date, provider_name=self.provider)
                    yield normalize.process_item(raw_item)

    @property
    def missing_rows(self):
        """So that analysts will be notified."""
        return parser.MISSING_ROWS
