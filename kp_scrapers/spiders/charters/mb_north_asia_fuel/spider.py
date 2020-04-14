import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.mb_north_asia_fuel import normalize, parser


class MBNorthAsiaFuel(CharterSpider, MailSpider):
    name = 'MB_NorthAsia_DPP'
    provider = 'Maven Brokers'
    version = '1.0.1'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """Parse report from e-mail and transform them into SpotCharter model.

        Args:
            mail (Mail):

        Returns:
            SpotCharter:

        """
        reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        for raw_row in self.select_body_html(mail).xpath('//text()').extract():

            raw_row = may_strip(raw_row)
            reported_date = self.parse_reported_date(raw_row) or reported_date

            # get arrival zone from previous line
            arrival_zone_match = re.match(r'[A-z0-9 ]+ AT ([A-z ]+)(?:\,)?.*', raw_row)
            if arrival_zone_match:
                arrival_zone = arrival_zone_match.group(1)
                continue

            row, headers = parser.split_row(raw_row)

            if row:
                raw_item = {headers[idx]: cell for idx, cell in enumerate(row)}
                raw_item.update(
                    arrival_zone=arrival_zone,
                    provider_name=self.provider,
                    reported_date=reported_date,
                )

                yield normalize.process_item(raw_item)

    @staticmethod
    def parse_reported_date(raw_reported_date):
        """Normalize raw reported date to a valid format string.

        FIXME charters loader assumes `dayfirst=True` when parsing, so we can't use ISO-8601

        Args:
            raw_date (str)

        Returns:
            str | None:

        """
        date_match = re.match(r'Sent: (\d{1,2}\s[A-z]+\s\d{4}.*)', raw_reported_date)
        if date_match:
            return parse_date(date_match.group(1), dayfirst=True).strftime('%d %b %Y')
