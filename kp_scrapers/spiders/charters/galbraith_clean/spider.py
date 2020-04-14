from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.galbraith_clean import normalize


class GalbraithCleanSpider(CharterSpider, MailSpider):
    name = 'Galbraith_Clean_West'
    provider = 'Galbraith'
    version = '0.1.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    # length of table row that will contain valid data
    full_rank = 11

    def parse_mail(self, mail):
        """Extract data from mail specified with filters in spider args.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:
        """
        # memomise reported date so it won't need to be called repeatedly later
        reported_date = self.extract_reported_date(mail)

        for raw_row in self.select_body_html(mail).xpath('//tr'):
            row = [
                may_strip(''.join(cell.xpath('.//text()').extract()))
                for cell in raw_row.xpath('.//td')
                if may_strip(''.join(cell.xpath('.//text()').extract()))
            ]

            # discard rows with incomplete data
            if len(row) < self.full_rank:
                continue

            raw_item = {str(cell_idx): cell for cell_idx, cell in enumerate(row)}
            raw_item.update(provider_name=self.provider, reported_date=reported_date)
            yield normalize.process_item(raw_item)

    @staticmethod
    def extract_reported_date(mail):
        """Extract reported date of mail.

        Source provides date in title like so:
            - FW: GALBRAITHS CLN WEST REPORT - 31/08/18

        Args:
            mail (Mail):

        Returns:
            str: "dd BBB YYYY" formatted date string
        """
        return parse_date(mail.envelope['subject'].split('-')[-1], dayfirst=True).strftime(
            '%d %b %Y'
        )
