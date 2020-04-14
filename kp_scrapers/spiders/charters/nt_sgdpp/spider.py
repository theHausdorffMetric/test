from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.nt_sgdpp import normalize


class NTSGDPPFixtures(CharterSpider, MailSpider):
    name = 'NT_SGDPP_Fixtures'
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
        """Parse report from e-mail and transform them into SpotCharter model.

        Args:
            mail (Mail):

        Returns:
            SpotCharter:

        """
        reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')
        start_processing = None

        for raw_row in self.select_body_html(mail).xpath('//tr'):
            row = [
                may_strip(''.join(cell.xpath('.//text()').extract()))
                for cell in raw_row.xpath('.//td')
                if ''.join(cell.xpath('.//text()').extract())
            ]
            if 'MEG-RSEA-INDIA' in row:
                start_processing = True

            if 'PROJECTS' in row:
                start_processing = False

            # remove unnessary rows
            if len([ele for ele in row if ele]) <= 3:
                continue

            # some filler rows have the following char
            if 'Â' in row:
                continue

            if start_processing:
                raw_item = {str(idx): cell for idx, cell in enumerate(row)}
                raw_item.update(provider_name=self.provider, reported_date=reported_date)

                yield normalize.process_item(raw_item)
