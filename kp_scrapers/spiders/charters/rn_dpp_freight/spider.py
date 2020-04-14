from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.rn_dpp_freight import normalize


# 0-based indexing
CHARTERER_COLUMN_IDX = 2
VESSEL_COLUMN_IDX = 6

# row may not contain fewer elements than this threshold
ROW_LENGTH_THRESHOLD = 7


class RNDPPFreightSpider(CharterSpider, MailSpider):
    name = 'RN_DPP_Freight'
    provider = 'SeaChar'
    version = '0.1.1'

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
        # provider doesn't include `reported_date` in email body; we guess it from mail headers
        reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        table = self.select_body_html(mail).xpath('//table//tr')
        raw_rows = [row.xpath('.//text()').extract() for row in table]

        is_relevant_data = False
        for row in raw_rows:
            row = [may_strip(col) for col in row if may_strip(col)]
            # # string denotes start of relevant table data
            if 'Cargoes & Fixtures' in ''.join(row):
                is_relevant_data = True

            # check is we are in the relevant html table row
            if not is_relevant_data or not row:
                continue

            # obtain headers
            if 'Vessel' in ''.join(row):
                header = row.copy()
                continue

            # check if row contains valid charter
            if len(row) <= ROW_LENGTH_THRESHOLD:
                continue

            # process raw row into a raw dict
            raw_item = {header[idx]: col for idx, col in enumerate(row)}
            # contexutalise raw item with some metadata
            raw_item.update(provider_name=self.provider, reported_date=reported_date)
            yield normalize.process_item(raw_item)
