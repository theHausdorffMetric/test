from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.shiplinks import normalize


HEADER_ROW_SIGN = 'Charterer'
MIN_COL_NUM = 6


class ShiplinksSpider(CharterSpider, MailSpider):
    name = 'SL_Fixtures_Dirty'
    provider = 'Shiplinks'
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

        headers = []
        for row_html in body.xpath('//tr'):
            # td tag may contains multiple text tags, join them
            raw_row = [''.join(td.xpath('.//text()').extract()) for td in row_html.xpath('./td')]
            row = [may_strip(x) for x in raw_row if may_strip(x)]

            if HEADER_ROW_SIGN in row:
                headers = row
                continue

            if headers and len(row) >= MIN_COL_NUM:
                raw_item = {headers[idx]: cell for idx, cell in enumerate(row)}
                raw_item.update(self.meta_field)

                yield normalize.process_item(raw_item)

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
