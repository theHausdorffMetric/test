import xlrd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.ms_usweekly import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


class MSUSSSpider(ShipAgentMixin, MailSpider):

    name = 'MS_USWeekly'
    provider = 'MS'
    version = '1.1.2'

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
        for attachment in mail.attachments():
            # each xls file only has one sheet
            sheet = xlrd.open_workbook(
                file_contents=attachment.body, on_demand=True
            ).sheet_by_index(0)

            # store state of the table, in order to get relevant rows to extract
            is_relevant = False
            for row in sheet.get_rows():
                row = [
                    xldate_to_datetime(cell.value, sheet.book.datemode).isoformat()
                    if is_xldate(cell)
                    else cell.value
                    for cell in row
                ]

                # discard irrelavant rows until we see the start pattern
                if any(sub in row for sub in ('Voyage Reference', 'Vessel Name')):
                    is_relevant = True
                    header = row
                    continue

                # sanity check using first element in case of empty row
                if is_relevant and row[0]:
                    raw_item = {head: row[idx] for idx, head in enumerate(header)}
                    # contextualise raw item with some meta info
                    raw_item.update(
                        reported_date=to_isoformat(mail.envelope['date']),
                        provider_name=self.provider,
                    )
                    yield from normalize.process_item(raw_item)
