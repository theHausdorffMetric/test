from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.banchero_vlcc import normalize


class BancheroCostaVLCCSpider(CharterSpider, MailSpider):
    name = 'BCR_Fixtures_VLCC'
    provider = 'Banchero'
    version = '1.0.1'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """

        Args:
            mail (Mail):

        Returns:
            SpotCharter:

        """
        reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')
        start_processing = False if 'wake up' not in mail.envelope['subject'].lower() else True

        for tr_sel in self.select_body_html(mail).xpath('//tr'):
            row = [
                may_strip(''.join(td_sel.xpath('.//text()').extract()))
                for td_sel in tr_sel.xpath('.//td')
            ]

            if 'med/black sea' in row:
                start_processing = True
                continue

            # wake up attachment has unneccesary tables
            if len(row) < 8:
                continue

            if start_processing:
                raw_item = {str(idx): row[idx] for idx, r in enumerate(row)}

                raw_item.update(provider_name=self.provider, reported_date=reported_date)
                yield normalize.process_item(raw_item)
