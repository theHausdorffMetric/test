from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.smi import normalize


class SouthportSpider(CharterSpider, MailSpider):
    name = 'SMI_Fixtures_Dirty'
    provider = 'SMI'
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
        start_processing = False
        for raw_row in body.xpath('//table//tr'):
            row = [may_strip(x) for x in raw_row.xpath('.//text()').extract() if may_strip(x)]
            if not row:
                continue

            if 'Charterer' in row:
                headers = row
                start_processing = True
                continue

            if start_processing and len(row) == len(headers):
                raw_item = {header: row[idx] for idx, header in enumerate(headers)}
                raw_item.update(**self.meta_field)

                yield normalize.process_item(raw_item)

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
