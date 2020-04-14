import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.gibson_dpp import normalize


BLACKLIST = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']


class GibsonCOBDPPSpider(CharterSpider, MailSpider):
    name = 'GB_COBFixtures_DPP'
    provider = 'Gibson'
    version = '1.1.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
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
        reported_date = self.extract_reported_date(mail.envelope['subject']) or parse_date(
            mail.envelope['date']
        ).strftime('%d %b %Y')
        start_processing = False
        for raw_row in self.select_body_html(mail).xpath('//tr'):
            row = [
                may_strip(''.join(cell.xpath('.//text()').extract()).lower())
                for cell in raw_row.xpath('.//td')
                if ''.join(cell.xpath('.//text()').extract())
            ]

            if len(row) >= 9 and 'charterer' in row[7]:
                header = row
                # sometimes the vessel col name is empty
                row[0] = 'vessel'
                start_processing = True
                continue

            if start_processing:
                # discard irrelevant rows
                if (len(row) < 9) or any(sub in row[0] for sub in BLACKLIST):
                    continue

                raw_item = {head: row[head_idx] for head_idx, head in enumerate(header)}
                raw_item.update(reported_date=reported_date, provider_name=self.provider)
                yield normalize.process_item(raw_item)

    @staticmethod
    def extract_reported_date(raw_subject):
        """Extract reported date of mail.

        Source provides date in title like so:
            - EA GIBSON DPP COB REPORT AND MARKET COMMENT THURSDAY 17TH JAN 2019

        Args:
            mail (Mail):

        Returns:
            str: "dd BBB YYYY" formatted date string
        """
        date_match = re.match(r'.*?(\d{1,2}\w{1,2}\s\w+\s\d{4})', raw_subject)
        if date_match:
            return parse_date(date_match.group(1), dayfirst=True).strftime('%d %b %Y')

    @property
    def missing_rows(self):
        return normalize.MISSING_ROWS
