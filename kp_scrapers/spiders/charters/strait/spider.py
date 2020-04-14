from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.strait import normalize


ROW_LEN_LIMIT = 10
FIXTURE_HEADER_SIGN = 'VESSEL'
STOP_PROCESSING_SIGN = 'OUTSTANDING CARGOES'


class StraitSpider(CharterSpider, MailSpider):
    name = 'SS_Charters_OIL'
    provider = 'Strait'
    version = '1.0.1'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    reported_date = None

    def parse_mail(self, mail):
        reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        header = None
        for tr_sel in self.select_body_html(mail).xpath('//tr'):
            row = [
                may_strip(''.join(td.xpath('.//text()').extract())) for td in tr_sel.xpath('.//td')
            ]

            if len(row) > ROW_LEN_LIMIT:
                continue

            if FIXTURE_HEADER_SIGN in row:
                header = row
                continue

            if STOP_PROCESSING_SIGN in row:
                return

            if header and len(row) == len(header):
                raw_item = {header[idx]: cell for idx, cell in enumerate(row)}
                raw_item.update(provider_name=self.provider, reported_date=reported_date)

                yield normalize.process_item(raw_item)
