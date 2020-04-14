import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.gibson_aframax import normalize


START_SIGN = 'Fixtures'
STOP_SIGN = 'Miscellaneous'


class GibsonAframaxSpider(CharterSpider, MailSpider):
    name = 'Gibson_Aframax_Fixtures'
    provider = 'Gibson'
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
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        body = self.select_body_html(mail)
        start_processing = False
        for raw_row in body.xpath('//text()').extract():
            raw_row = may_strip(raw_row)

            if START_SIGN in raw_row:
                start_processing = True
                continue

            if STOP_SIGN in raw_row:
                return

            if start_processing and raw_row:
                row = self.split_row(raw_row.upper())

                if row:
                    self.logger.info(f'Start processing fixtures row: {row}')
                    raw_item = {str(idx): cell for idx, cell in enumerate(row)}
                    raw_item.update(self.meta_field)

                    yield from normalize.process_item(raw_item)

                else:
                    self.logger.warning(
                        f'Discard the row that doesn\'t match the pattern:{raw_row}'
                    )

    @staticmethod
    def split_row(row):
        """Restore the cells in the row.

        Args:
            row (str): all capital

        Returns:
            List[str]:

        """
        # eg:
        # Seabravery 80kt nhc 11-12/10 CPC Med W/S 100 Shell
        # Signal Puma 80kt fo 10/10 Taman Med-Malta W/S 105-100 Coral - heating for owners account
        pattern = (
            # vessel
            r'(.+)\s'
            # size
            r'(\d{2,3}(?:KT)?)\s'
            # cargo
            r'([A-Z]+)\s'
            # lay can
            r'(\d{1,2}-\d{1,2}/\d{1,2}|\d{1,2}/\d{1,2}|[A-Z]{3,5}/\d{1,2})\s'
            # voyage
            r'([^\d]+)\s'
            # rate value
            r'(O/P|COA|RNR|W/S[^A-Z]+|WS[^A-Z]+)'
            # charterer and status (if any)
            r'(.*)'
        )

        _match = re.match(pattern, row)
        return list(_match.groups()) if _match else None

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
