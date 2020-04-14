import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.smi_clean import normalize


MISSING_ROWS = []
START_PROCESSING = ['UKC & Mediterranean', 'US Gulf & Atlantic Basin']
END_PROCESSING = ['QUOTE OF THE DAY']
IGNORE_ROWS = ['*PALMS', '----']


class SMICleanSpider(CharterSpider, MailSpider):
    name = 'SMI_Fixtures_Clean'
    provider = 'SMI'
    version = '1.0.1'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """Parse the mail body and generate events.

        Args:
            mail (Mail):

        Returns:
            SpotCharter:

        """
        reported_date = self.extract_reported_date(mail.envelope['subject'])
        start_processing = False

        body = self.select_body_html(mail)
        for row_html in body.xpath('//span/span'):
            raw_row = may_strip(''.join(row_html.xpath('.//text()').extract()))
            if any(sub in raw_row for sub in START_PROCESSING):
                start_processing = True
                continue

            if any(sub in raw_row for sub in END_PROCESSING):
                start_processing = False
                continue

            if start_processing:
                row = self.split_row(raw_row)
                if row:
                    raw_item = {str(idx): cell for idx, cell in enumerate(row)}
                    raw_item.update(provider=self.provider, reported_date=reported_date)
                    yield normalize.process_item(raw_item)

    @staticmethod
    def split_row(raw_row):
        """Restore the cells.

        Args:
            raw_row (str):

        Returns:
            List[str]:

        """
        pattern = (
            # vessel
            r'(.*)\s'
            # size
            r'(\d+)\s'
            # cargo
            r'([A-z]+)\s'
            # departure
            r'([A-z.\s]+)\/'
            # arrival
            r'([A-z-.\s]+)\s'
            # laycan
            r'([A-z0-9\/-]+)\s'
            # rate
            r'(RNR|O\/P|COA|WS\s?.*?|\$.*?)\s'
            # charterer
            r'(.*)\s'
            # status
            r'(.*)'
        )

        # pre-process to make rate field correctly matched
        pre_row = raw_row.upper().replace('USD ', 'USD').replace('X-UKC', 'UKC/UKC')
        match = re.match(pattern, pre_row)

        if match:
            return list(match.groups())
        elif raw_row and not any(sub in raw_row for sub in IGNORE_ROWS):
            MISSING_ROWS.append(raw_row)

    @property
    def missing_rows(self):
        return MISSING_ROWS

    @staticmethod
    def extract_reported_date(raw_string):
        return parse_date(raw_string.partition('-')[-1]).strftime('%d %b %Y')
