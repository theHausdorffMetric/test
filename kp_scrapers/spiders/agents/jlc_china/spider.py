import xlrd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.jlc_china import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


# string pattern identifying appropriate table header
HEADER_PATTERN = 'Vessel'
# 0-based index of column where we can evaluate within to see if there is relevant data
RELEVANT_ROW_INDICATOR = 3
# 0-based index of sheet with relevant tabular data
RELEVANT_SHEET_INDEX = 1


class JlcChinaOilSpider(ShipAgentMixin, MailSpider):
    name = 'JLC_China_Oil'
    provider = 'JLC'
    version = '0.1.0'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """Extract data from each mail matched by the query spider argument.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:

        """
        # memoise reported_date so it won't need to be called repeatedly later
        reported_date = to_isoformat(mail.envelope['date'])

        for attachment in mail.attachments():
            # only one sheet in the excel file is relevant
            sheet = xlrd.open_workbook(
                file_contents=attachment.body, on_demand=True
            ).sheet_by_index(RELEVANT_SHEET_INDEX)

            for idx, row in enumerate(sheet.get_rows()):
                row = [
                    xldate_to_datetime(cell.value, sheet.book.datemode).isoformat()
                    if is_xldate(cell)
                    else cell.value
                    for cell in row
                ]

                # initialise headers and standardise as headers may have variations
                if HEADER_PATTERN in str(row):
                    header = [cell for cell in row if cell]
                    continue

                # remove empty, useless rows
                if not row[RELEVANT_ROW_INDICATOR]:
                    continue

                raw_item = {head: row[col_idx] for col_idx, head in enumerate(header)}
                # contextualise raw item with metadata
                raw_item.update(provider_name=self.provider, reported_date=reported_date)
                yield normalize.process_item(raw_item)
