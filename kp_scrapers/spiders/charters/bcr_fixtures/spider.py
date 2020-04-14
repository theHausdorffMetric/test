from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.bcr_fixtures import normalize


class BancheroFixturesSpider(CharterSpider, MailSpider):
    name = 'BCR_Fixtures'
    provider = 'Banchero'
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
        """The method will be called for every mail the search_term matched.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:

        """
        # memoise and use received timestamp of email as default reported date
        reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        if 'dty' in mail.envelope['subject'].lower():
            complete_header_row = 9
            headers = [
                'vessel',
                'status',
                'qty',
                'grade',
                'lay_can_from',
                'load',
                'discharge',
                'rate',
                'charterer',
            ]

            yield from self.parse_attachment(mail, complete_header_row, reported_date, headers)

        if 'cln' in mail.envelope['subject'].lower():
            complete_header_row = 10
            # headers split into 2, declaring headers here to filter out
            # unnessary rows later on
            headers = [
                'vessel',
                'qty',
                'grade',
                'lay_can_from',
                'lay_can_to',
                'load',
                'discharge',
                'rate',
                'charterer',
                'status',
            ]

            yield from self.parse_attachment(mail, complete_header_row, reported_date, headers)

    def parse_attachment(self, mail_body, row_length, rpt_string, head):
        start_processing = False
        for raw_row in self.select_body_html(mail_body).xpath('//tr'):
            row = [
                may_strip(''.join(cell.xpath('.//text()').extract()))
                for cell in raw_row.xpath('.//td')
                if ''.join(cell.xpath('.//text()').extract())
            ]

            # remove unecessary rows
            if not row:
                continue

            # detect row to start processing
            if row[0] == 'VESSEL':
                start_processing = True
                continue

            if start_processing:
                if len(row) == row_length:
                    raw_item = {h: row[idx] for idx, h in enumerate(head) if h}
                    raw_item.update(provider_name=self.provider, reported_date=rpt_string)
                    yield normalize.process_item(raw_item)

    @property
    def missing_rows(self):
        return normalize.MISSING_ROWS
