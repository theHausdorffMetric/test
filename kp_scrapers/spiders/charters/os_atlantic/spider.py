import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.os_atlantic import normalize


PRE_PROCESS_MAPPING = {' RNR ': ' 0 ', ' COA ': ' 1 ', 'USD ': 'USD'}


class OsAtlanticSpider(CharterSpider, MailSpider):
    name = 'OS_Atlantic_Fixtures'
    provider = 'Ocean Shipbrokers'
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
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        body = self.select_body_html(mail)
        for row_html in body.xpath('//p'):
            # we divide the row by <p> tag, however, some rows are in the same <p> tag, usually,
            # they are separated by two <br> tag, therefore in here we use `\r\n\r\n`, as single
            # <br> is spotted in a row.
            # this is not a very good solution, but due to the format is quite inconsistent, there's
            # no other way to detect the rows elegantly.
            rows = ''.join(row_html.xpath('.//text()').extract()).split('\r\n\r\n')
            for raw_row in rows:
                row = self.split_row(may_strip(raw_row))
                if row:
                    raw_item = {str(idx): cell for idx, cell in enumerate(row)}
                    raw_item.update(self.meta_field)

                    yield normalize.process_item(raw_item)

    @staticmethod
    def split_row(row):
        """Split the row with regex.

        Row header:
            vessel / size (cargo) / lay can / voyage / rate / charterer (status)

        Row format:
            - ENERGY TRIUMPH 130FO 18-19/09 BALTIC/USG W53.75 TRAFIGURA =INC HEAT=
            - S’GOL CAZENGA 130 26/09 ANGOLA/POINT TUPPER RNR P66 =FLD=
            - ITHAKI WARRIOR O/O130 26-27/09 ESCRAVOS/UKCM W72.5 ST

        To reduce complexity, we'll replace RNR and COA to numerical format first, and will restore
        them in normalizing.

        Tested on report from 17 Aug 2018 - 17 Sep 2018, it works well.

        Args:
            row (str):

        Returns:
            List[str]:

        """
        # pre process, to reduce complexity and increase correctness of the regex
        for alias in PRE_PROCESS_MAPPING:
            row = row.replace(alias, PRE_PROCESS_MAPPING[alias])

        pattern = (
            # vessel
            r'(.+?)\s+'
            # size (cargo)
            r'(\d{2,3})([A-Z]+)?\s'
            # lay can
            r'(\d{1,2}.\d{1,2}.\d{1,2}|\d{1,2}.\d{1,2}|[A-Z]{3,5}.\d{1,2})\s'
            # voyage
            r'([^\d]+)\s'
            # rate
            r'([^\s]+)\s'
            # charterer (status)
            r'(.+)'
        )

        _match = re.match(pattern, row)
        return list(_match.groups()) if _match else None

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
