import xlrd

from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.affinity import normalize


# `Charterer` is used since it's reliably present in both dry/tanker fixtures header
HEADER_PATTERN = 'Charterer'
# index of row to indicate if the row is empty and should be skipped
EMPTY_ROW_INDICATOR_INDEX = 3


class AffinitySpider(CharterSpider, MailSpider):

    name = 'AF_Fixtures'
    provider = 'Affinity'
    version = '1.0.0'
    produces = [DataTypes.SpotCharter]

    spider_settings = {
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True
    }

    def parse_mail(self, mail):
        """Extract mail found with specified email filters in spider arguments.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:

        """
        for attachment in mail.attachments():
            # each xlsx file by this provider will only have one sheet
            sheet = xlrd.open_workbook(
                file_contents=attachment.body, on_demand=True
            ).sheet_by_index(0)

            for idx, row in enumerate(sheet.get_rows()):
                row = [
                    xldate_to_datetime(cell.value, sheet.book.datemode).isoformat()
                    if is_xldate(cell)
                    else cell.value
                    for cell in row
                ]

                # remove empty filler rows before and after the main data table
                if not row[EMPTY_ROW_INDICATOR_INDEX]:
                    continue

                # initialise headers
                if HEADER_PATTERN in row:
                    header = row
                    continue

                # sanity check, just in case we somehow miss the headers
                # due to changes in table structures
                if 'header' in locals():
                    raw_item = {head: row[idx] for idx, head in enumerate(header) if head}
                    # contextualise raw item with metadata
                    raw_item.update(provider_name='Affinity', spider_name=self.name)
                    yield normalize.process_item(raw_item)


class AffinityDrySpider(AffinitySpider):
    # COAL fixtures
    name = 'AF_Fixtures_Dry'


class AffinityTankerSpider(AffinitySpider):
    # OIL/CPP fixtures
    name = 'AF_Fixtures_Tanker'
