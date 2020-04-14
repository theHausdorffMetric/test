import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.dietze import normalize


class DietzeSpider(CharterSpider, MailSpider):
    name = 'DA_Fixtures'
    provider = 'Dietze & Associates'
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
        """Parse the mail body and generate events.

        Args:
            mail (Mail):

        Returns:
            SpotCharter:

        """
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        body = self.select_body_html(mail)
        for row_html in body.xpath('//p'):
            raw_row = may_strip(''.join(row_html.xpath('.//text()').extract()))
            row = self.split_row(raw_row)

            if row:
                raw_item = {str(idx): cell for idx, cell in enumerate(row)}
                raw_item.update(self.meta_field)

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
            r'(.+)\s'
            # size
            r'(\d+)\s'
            # cargo (optional)
            r'([A-Z]{,4}\s)?'
            # voyage
            r'([^\d]+)'
            # laycan
            r'(\d{,2}/\d{,2}-[\d-]{,2}|\d{,2}/\d{,2}|\d{,2}/END|EARLY\s[A-Z]{,4})\s'
            # rate
            r'([A-Z\d,$.-]{2,})\s'
            # charterer - status (optional)
            r'(.*)'
        )

        # pre-process to make rate field correctly matched
        pre_row = raw_row.upper().replace('USD ', 'USD')
        match = re.match(pattern, pre_row)

        return list(match.groups()) if match else None

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
