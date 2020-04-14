import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.nt_dirty_east import normalize


class N2TankersDirtyEastSpider(CharterSpider, MailSpider):
    name = 'NT_TankersEast_Fixtures'
    provider = 'N2 Tankers'
    version = '1.0.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
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

        for _raw_row in self.select_body_html(mail).xpath('//p//text()').extract():
            if 'tankers dirty' in _raw_row.lower():
                reported_date = self.extract_reported_date(_raw_row)
                break

        for raw_row in self.select_body_html(mail).xpath('//tr'):

            row = [
                may_strip(''.join(cell.xpath('.//text()').extract()))
                for cell in raw_row.xpath('.//td')
                if ''.join(cell.xpath('.//text()').extract())
            ]

            # extract fixtures rows, there is another table called outstanding
            # cargoes which has lesser columns and not required
            if len(row) < 10:
                continue

            if not row[0]:
                continue

            raw_item = {str(cell_idx): cell for cell_idx, cell in enumerate(row)}
            raw_item.update(provider_name=self.provider, reported_date=reported_date)
            yield normalize.process_item(raw_item)

    @staticmethod
    def extract_reported_date(raw_rpt_date):
        """Extract reported date from report

        Source provides date in report like so:
            - TANKERS DIRTY EAST MARKET UPDATE – 24TH APR 2019

        Although source provides data, it contains unicode characters,
        For example,
        =?UTF-8?Q?Fwd=3A_TANKERS_DIRTY_EAST_MARKET_UPDATE_=E2=80=93_24TH_APR_2?==?UTF-8?Q?019?=
        hence extracting from report would be easier

        Args:
            mail (Mail):

        Returns:
            str: "dd BBB YYYY" formatted date string
        """
        _match = re.match(r'(?:[\W\w]+)\s([0-9]+[A-z]+\s[A-z]+\s[0-9]+)$', raw_rpt_date)
        if _match:
            return parse_date(_match.group(1), dayfirst=True).strftime('%d %b %Y')

    @property
    def missing_rows(self):
        return normalize.MISSING_ROWS
