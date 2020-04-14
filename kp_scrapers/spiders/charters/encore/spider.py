from dateutil.parser import parse as parse_date

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.encore import normalize, parser


class EncoreSpider(CharterSpider, MailSpider):
    name = 'Encore_Fixtures'
    provider = 'Encore Shipping'
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

        table = list(parser.get_fixtures_data_table(body))
        for row in parser.restore_cells_for_row(table):
            raw_item = {str(idx): cell for idx, cell in enumerate(row)}
            raw_item.update(**self.meta_field)

            yield normalize.process_item(raw_item)

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}

    @property
    def missing_rows(self):
        return parser.MISSING_ROWS
