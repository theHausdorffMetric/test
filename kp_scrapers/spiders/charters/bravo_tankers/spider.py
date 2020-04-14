from dateutil.parser import parse as parse_date

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.bravo_tankers import normalize, parser


class BravoTankersSpider(CharterSpider, MailSpider):
    name = 'BT_DailyFixtures_Crude'
    provider = 'Bravo Tankers'
    version = '1.1.0'
    produces = [DataTypes.SpotCharter]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """Extract data from each mail matched by the query spider argument.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:

        """
        # NOTE report actually provides `reported_date`, but often riddled with valid typos
        # e.g. 18/02/2018 even when report data refers to Feb 2019
        # therefore, we use the mail sent date instead
        reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        body = self.select_body_html(mail)
        for row in parser.get_data_table(body):
            raw_item = parser.map_row_to_dict(
                row, provider_name=self.provider, reported_date=reported_date
            )
            yield normalize.process_item(raw_item)
