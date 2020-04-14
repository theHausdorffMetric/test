import xlrd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.bc_usmonthly import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


class BcUsMonthlySpider(ShipAgentMixin, MailSpider):
    name = 'BC_USMonthly'
    provider = 'B&C'
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
            # sheet 0 contains historical info, but we want monthly info in sheet 1
            sheet = xlrd.open_workbook(
                file_contents=attachment.body, on_demand=True
            ).sheet_by_index(1)

            # store state of the table, in order to get relevant rows to extract
            for idx, row in enumerate(sheet.get_rows()):
                row = [
                    xldate_to_datetime(cell.value, sheet.book.datemode).isoformat()
                    if is_xldate(cell)
                    else cell.value
                    for cell in row
                ]

                # initialise headers
                if idx == 0:
                    header = row
                    continue

                raw_item = {head: row[idx] for idx, head in enumerate(header)}
                # contextualise raw item with meta info
                raw_item.update(reported_date=reported_date, provider_name=self.provider)
                yield normalize.process_item(raw_item)
