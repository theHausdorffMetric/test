from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.rn_dpp import normalize


REQUIRED_ROW_LEN = 9
VESSEL_COL_IDX = 0
CHARTERER_COL_IDX = 1

START_PROCESSING_SIGN = ['SUEZMAX', 'UPDATE']
HEADER_SIGN = 'VESSEL'


class RnDppSpider(CharterSpider, MailSpider):
    name = 'RN_DPP'
    provider = 'Howe Rob'
    version = '0.2.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    reported_date = None
    missing_rows = []

    def parse_mail(self, mail):
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        processing_started = False
        header = None
        for row_selector in self.select_body_html(mail).xpath('//tr'):
            row = [
                may_strip(''.join(cell.xpath('.//text()').extract())).upper()
                for cell in row_selector.xpath('./td')
            ]

            for x in START_PROCESSING_SIGN:
                if x in ''.join(row):
                    processing_started = True
                    break

            if HEADER_SIGN in row:
                header = row
                continue

            if processing_started:
                is_valid_row = header and len(header) == len(row)
                has_data = row[VESSEL_COL_IDX] and row[CHARTERER_COL_IDX]
                if is_valid_row and has_data:
                    raw_item = {str(idx): cell for idx, cell in enumerate(row)}
                    raw_item.update(self.meta_field)

                    yield normalize.process_item(raw_item)

                elif len(row) >= REQUIRED_ROW_LEN and has_data:
                    self.missing_rows.append(' '.join(row))

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
