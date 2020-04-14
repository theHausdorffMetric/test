from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.gibson_vlcc import normalize


FIXTURES = 'FIXTURES:'
ROW_LENGTH = 7


class GibsonVLCCSpider(CharterSpider, MailSpider):
    name = 'Gibson_VLCC_Fixtures'
    provider = 'Gibson'
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

        table = self.select_body_html(mail).xpath('//table[2]')
        start_processing = False
        for raw_row in table.xpath('.//tr'):
            row = [may_strip(x) for x in raw_row.xpath('./td//text()').extract() if may_strip(x)]

            if any(FIXTURES in cell for cell in row):
                start_processing = True
                continue

            if start_processing and len(row) == ROW_LENGTH:
                raw_item = {str(idx): cell for idx, cell in enumerate(row)}
                raw_item.update(self.meta_field)

                yield normalize.process_item(raw_item)

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
