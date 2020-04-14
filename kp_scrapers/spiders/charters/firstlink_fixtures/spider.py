import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.firstlink_fixtures import normalize


MISSING_ROWS = []


class FirstlinkFixtureSpider(CharterSpider, MailSpider):

    name = 'FL_Fixtures_Clean'
    provider = 'Firstlink'
    version = '1.0.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """Extract raw data from the given mail.

        Because data may be given in either the email body or a .docx attachment,
        there is a need to dispatch the extracting functions separately.

        Args:
            mail (Mail): see `lib.services.mail.Mail` for details

        Yields:
            Dict[str, str]:
        """
        start_processing = False
        reported_date = self.extract_reported_date(mail.envelope['subject'])
        for raw_row in self.select_body_html(mail).xpath('//tr'):
            row = [
                may_strip(''.join(cell.xpath('.//text()').extract()))
                for cell in raw_row.xpath('.//td')
                if ''.join(cell.xpath('.//text()').extract())
            ]

            if row and 'VESSEL' in row[0].upper():
                start_processing = True
                header = row
                continue

            if start_processing:
                if len(header) == len(row):
                    raw_item = {head.upper(): row[head_idx] for head_idx, head in enumerate(header)}
                    raw_item.update(reported_date=reported_date, provider_name=self.provider)
                    yield normalize.process_item(raw_item)
                else:
                    MISSING_ROWS.append(' '.join(row))

    @staticmethod
    def extract_reported_date(rpt_string):
        _match = re.match(r'.*-(.*)', rpt_string)
        if _match:
            return parse_date(_match.group(1), dayfirst=True).strftime('%d %b %Y')

    @property
    def missing_rows(self):
        """So that analysts will be notified."""
        return MISSING_ROWS
