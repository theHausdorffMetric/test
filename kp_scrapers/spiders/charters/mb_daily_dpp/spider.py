import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.mb_daily_dpp import normalize


START_SIGN = '[FIXTURE]'
STOP_SIGN = '[OUTSTANDING CARGOES]'
COMPLETE_DATA_ROW_LENGTH = 10


class MavenDailyDPPSpider(CharterSpider, MailSpider):
    name = 'MB_DailyFixtures_DPP'
    provider = 'Maven Brokers'
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
        """Parse report from e-mail and transform them into SpotCharter model.

        Args:
            mail (Mail):

        Returns:
            SpotCharter:

        """
        self.reported_date = self.extract_reported_date(mail.envelope['subject']) or parse_date(
            mail.envelope['date']
        ).strftime('%d %b %Y')
        start_processing = False

        for raw_row in self.select_body_html(mail).xpath('//tr'):
            row = [
                may_strip(''.join(cell.xpath('.//text()').extract()))
                for cell in raw_row.xpath('.//td')
                if ''.join(cell.xpath('.//text()').extract())
            ]

            if START_SIGN in row[0]:
                start_processing = True
                continue

            if STOP_SIGN in row[0]:
                start_processing = False
                continue

            if start_processing:
                # discard irrelevant rows
                if row[0] == 'Â' or row[0] == '':
                    continue

                if 'vessel' in row[0].lower():
                    header = row
                    continue

                # charter and remarks column might not exist
                if len(row) != COMPLETE_DATA_ROW_LENGTH:
                    row_length = len(row)
                    diff = COMPLETE_DATA_ROW_LENGTH - row_length
                    for i in range(0, diff):
                        row.append('')

                raw_item = {head: row[head_idx] for head_idx, head in enumerate(header)}
                raw_item.update(reported_date=self.reported_date, provider_name=self.provider)

                yield normalize.process_item(raw_item)

    @staticmethod
    def extract_reported_date(raw_subject):
        """Extract reported date of mail.

        Source provides date in title like so:
            - DAILY DPP MARKET REPORT [13/JAN/2019]

        Args:
            mail (Mail):

        Returns:
            str: "dd BBB YYYY" formatted date string
        """
        date_match = re.match(r'.*\[(\d{1,2}\/[A-z]+\/\d{4})\]', raw_subject)
        if date_match:
            return parse_date(date_match.group(1), dayfirst=True).strftime('%d %b %Y')
