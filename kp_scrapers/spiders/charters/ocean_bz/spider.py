from scrapy import Request

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.ocean_bz import normalize


class OceanBZMailSpider(CharterSpider, MailSpider):
    name = 'OBZ_Fixtures'
    provider = 'Ocean BZ'
    version = '1.0.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):

        body = self.select_body_html(mail)
        if body.xpath('//h1').extract():
            yield Request(
                url=body.xpath('//h1//a/@href').extract_first(), callback=self.parse_email_body_r
            )
        else:
            yield from self.parse_email_body_r(
                response=None, e_body=body, email_rpt_date=mail.envelope['subject']
            )

    def parse_email_body_r(self, response, e_body=None, email_rpt_date=None):
        # memoise reported date so it won't need to be assigned repeatedly again
        response_to_use = e_body if e_body else response
        reported_date = may_strip(''.join(response.xpath('//caption//text()').extract()))
        rpt_date_to_use = email_rpt_date if email_rpt_date else reported_date

        for raw_row in response_to_use.xpath('//table//tr'):
            row = [may_strip(x) for x in raw_row.xpath('.//text()').extract() if may_strip(x)]

            if len(row) < 8:
                continue

            if 'chartr' in row[7].lower():
                headers = row
                continue

            raw_item = {header.lower(): row[idx] for idx, header in enumerate(headers)}
            raw_item.update({'provider_name': self.provider, 'reported_date': rpt_date_to_use})
            yield normalize.process_item(raw_item)
