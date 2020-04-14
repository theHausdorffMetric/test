from itertools import zip_longest
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.bloomberg import normalize_email, normalize_pdf


VESSEL_IDX = 0
LAY_CAN_IDX = 4
ARRIVAL_IDX = -1
LONG_ROW_LEN = 9
LONG_ROW_LAY_CAN_IDX = -4

HEADER = [
    'Vessel_name',
    'volume',
    'product',
    'departure_zone',
    'arrival_zone',
    'lay_can',
    'charterer',
    'charterer_status',
]


class BloombergSpider(CharterSpider, PdfSpider, MailSpider):
    name = 'BG_Fixtures_CPP'
    provider = 'Bloomberg'
    version = '1.0.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    tabula_options = {'--guess': [], '--pages': ['all'], '--stream': []}

    reported_date = None

    def parse_mail(self, mail):
        """Entry point of BG_Fixtures_CPP spider.

        Args:
            mail (Mail):

        Yields:
            SpotCharter:

        """
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        body = self.select_body_html(mail)

        for row in body.xpath('//tr'):
            col = row.xpath('.//td')
            # all the tables will have atleast 5 columns
            if len(col) >= 5:
                yield from self.parse_email_body(
                    [may_strip(item) for item in col.xpath('.//text()').extract()]
                )

        # TODO The pdf format obtained is changing a lot, so far now we are skipping
        # Once the pdf format is stable we can make use of this by doing required changes
        # for attachment in mail.attachments():
        #     if attachment.is_pdf:
        #         yield from self.parse_pdf(attachment.body)

    def parse_email_body(self, row):
        """Parse the email body
        Args:
            List[str]
        Yield:
            Dict[str, Any]
        """
        raw_item = dict((zip_longest(HEADER, row)))
        raw_item.update(self.meta_field)

        yield normalize_email.process_item(raw_item)

    def parse_pdf(self, body):
        """Parse pdf table.

        Args:
            body (Body): attachment body
        Yields:
            SpotCharter:
        """
        table = self.extract_pdf_io(body, **self.tabula_options)

        start_processing = False
        for row in table:
            if 'Tanker' in ''.join(row):
                start_processing = True
                continue

            if start_processing and len(row) == LONG_ROW_LEN:
                row = self.parse_long_row(row)
                if not row:
                    continue

            if start_processing and row[VESSEL_IDX] and row[LAY_CAN_IDX]:
                raw_item = {str(idx): cell for idx, cell in enumerate(row)}
                raw_item.update(self.meta_field)

                yield normalize_pdf.process_item(raw_item)

    def parse_long_row(self, row):
        """Long row is different from the rest, need to detect separately.

        Regex link: https://regex101.com/r/gOi0ga/1/

        Args:
            row (List[str]):

        Returns:
            List[str]:

        """
        pattern = r'(.+?)\s(\d{2,3})\s([A-Z]+)\s(.+?)\s([A-Z]{3,4}\. \d{1,2})\s?([A-Z]+)?'
        str_row = may_strip(' '.join(row)).upper()

        _match = re.match(pattern, str_row)

        if _match:
            new_row = list(_match.groups())
            new_row.insert(ARRIVAL_IDX, '')
            return new_row

        else:
            if row[LONG_ROW_LAY_CAN_IDX]:
                self.logger.warning(f'Do we miss this row? {str_row}')

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
