from dateutil.parser import parse as parse_date

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.mb_arb import normalize, parser


class MbArbSpider(CharterSpider, MailSpider):
    name = 'MB_Arb'
    provider = 'Maven Brokers'
    version = '1.0.0'
    produces = [DataTypes.SpotCharter]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    reported_date = None

    def parse_mail(self, mail):
        """Parse mail body into spot charter event.

        Args:
            mail (Mail):

        Returns:
            SpotCharter | None:

        """
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')
        body = self.select_body_html(mail)
        for row in parser.get_data_table(body):
            raw_item = {str(idx): cell for idx, cell in enumerate(row)}
            raw_item.update(self.meta_field)

            yield normalize.process_item(raw_item)

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
