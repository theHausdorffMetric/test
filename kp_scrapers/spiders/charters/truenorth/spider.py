import re

from dateutil.parser import parse as parse_date

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.truenorth import normalize


class TrueNorthSpider(CharterSpider, MailSpider):
    name = 'TN_Fixtures'
    provider = 'TrueNorth'
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
        """Parse mail data and transform into SpotCharter Model.

        Args:
            mail (Mail):

        Returns:
            SpotCharter:

        """
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')
        body = self.select_body_html(mail)

        for row_html in body.xpath('//p'):
            raw_row = ''.join(row_html.xpath('.//text()').extract())
            row = self.split_row(raw_row)

            if row:
                raw_item = {str(idx): cell for idx, cell in enumerate(row)}
                raw_item.update(self.meta_field)

                yield normalize.process_item(raw_item)

    @staticmethod
    def split_row(row):
        """Using regex to split the row.

        For example matches, check out: https://regex101.com/r/mtkfWn/1/

        Args:
            row (str):

        Returns:
            List[str]

        """
        # example: DALIAN 260 16-18/10 WAF / CHINA WS55 CNOOC
        pattern = (
            # vessel
            r'(.+)\s'
            # size
            r'(\d+)'
            # cargo (optional)
            r'([A-Z]+)?\s'
            # lay can
            r'([A-Z]{,5}/\d{1,2}|\d{1,2}/\d{1,2}|\d{1,2}-\d{1,2}/\d{1,2})'
            # departure zone
            r'\s(.+)/'
            # arrival zone
            r'([A-Z-–\s]+?)\s'
            # rate value
            r'(RNR|\S+)\s'
            # charterer - status (optional)
            r'(.+)'
        )

        row = row.replace('â€“', '-').replace('–', '-').replace('Â', '')

        match = re.match(pattern, row)
        return list(match.groups()) if match else None

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
