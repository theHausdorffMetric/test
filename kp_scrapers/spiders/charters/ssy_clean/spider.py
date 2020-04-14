import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.ssy_clean import normalize


class SSYCleanSpider(CharterSpider, MailSpider):
    name = 'SSY_Clean_West'
    provider = 'SSY'
    version = '0.1.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """Extract data from mail specified with filters in spider args.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:
        """
        # memomise reported date so it won't need to be called repeatedly later
        reported_date = self.extract_reported_date(mail.envelope['subject'])

        for raw_row in self.select_body_html(mail).xpath('//tr'):
            row = [
                may_strip(''.join(cell.xpath('.//text()').extract()))
                for cell in raw_row.xpath('.//td')
                if may_strip(''.join(cell.xpath('.//text()').extract()))
            ]

            # discard filler rows
            if len(row) <= 7 or 'charterer' in row[0].lower():
                continue

            if 'charterer' in row[2].lower():
                header = row
                continue

            raw_item = {header[cell_idx].lower(): cell for cell_idx, cell in enumerate(row)}
            raw_item.update(provider_name=self.provider, reported_date=reported_date)
            yield normalize.process_item(raw_item)

    @staticmethod
    def extract_reported_date(raw_rpt_date):
        """Extract reported date of mail.

        Source provides date in title like so:
            - FW: GALBRAITHS CLN WEST REPORT - 31/08/18

        Args:
            mail (Mail):

        Returns:
            str: "dd BBB YYYY" formatted date string
        """
        match_rptd_date = re.search(r'^.*?(\d.*\d{4})$', raw_rpt_date.strip())

        if match_rptd_date:
            rptd_date = match_rptd_date.group(1)
            return parse_date(rptd_date, dayfirst=True).strftime('%d %b %Y')

    @property
    def missing_rows(self):
        return normalize.MISSING_ROWS
