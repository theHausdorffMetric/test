from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.mavensing import normalize, parser


NUMBER_OF_VALID_COLUMNS = 7


class MavensingSpider(CharterSpider, MailSpider):
    name = 'MB_SingFuel_Fixtures_DPP'
    provider = 'Maven Brokers'
    version = '1.0.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')
        body = self.select_body_html(mail)
        for row_sel in body.xpath('//text()'):
            row = [may_strip(cell) for cell in row_sel.extract().split('\xa0') if may_strip(cell)]
            # To avoid scrapping unwanted entries(all the important table has only 7 columns)
            if len(row) == NUMBER_OF_VALID_COLUMNS:
                raw_item = {
                    str(idx): cell
                    for idx, cell in enumerate(parser.extract_volume_product_date(row))
                }
                raw_item.update(self.meta_field)

                # To avoid parsing the header columns
                if 'VESSEL' in raw_item.values():
                    continue

                yield normalize.process_item(raw_item)

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
