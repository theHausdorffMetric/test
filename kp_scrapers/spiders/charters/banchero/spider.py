from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.banchero import normalize


START_PROCESSING_SIGN = 'FIXTURES'
HEADER_SIGN = 'VESSEL'
STOP_PROCESSING_SIGN = ['ON SUBS/GONE', 'OUTSTANDING CARGOES', 'CHARTERERS', 'OUSTANDING CARGOES']


class BancheroCostaSpider(CharterSpider, MailSpider):
    name = 'BC_Fixtures_OIL'
    provider = 'Banchero'
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
        """Entry point of BC_Fixtures_OIL spider.

        Args:
            mail (Mail):

        Returns:
            SpotCharter:

        """
        reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')
        body = self.select_body_html(mail)

        processing_started = False
        header = None
        for tr_sel in body.xpath('//tr'):
            raw_row = [
                ''.join(td_sel.xpath('.//text()').extract()) for td_sel in tr_sel.xpath('.//td')
            ]
            row = [may_strip(cell) for cell in raw_row]

            if START_PROCESSING_SIGN in row:
                processing_started = True
                continue

            if any(sign in ''.join(row) for sign in STOP_PROCESSING_SIGN):
                return

            if processing_started and HEADER_SIGN in row:
                header = row
                continue

            if processing_started and header and len(row) >= len(header):
                raw_item = {h: row[idx] for idx, h in enumerate(header) if h}
                raw_item.update(provider_name=self.provider, reported_date=reported_date)

                yield normalize.process_item(raw_item)
