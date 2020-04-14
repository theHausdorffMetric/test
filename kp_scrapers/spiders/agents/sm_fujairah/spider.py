import xlrd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.sm_fujairah import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


class SMFujairahSpider(ShipAgentMixin, MailSpider):
    name = 'SM_Fujairah'
    provider = 'Seamaster'
    version = '0.1.1'
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
        start_processing = False

        for attachment in mail.attachments():
            sheet = xlrd.open_workbook(
                file_contents=attachment.body, on_demand=True
            ).sheet_by_index(0)

            # store state of the table, in order to get relevant rows to extract
            for idx, raw_row in enumerate(sheet.get_rows()):
                row = format_cells_in_row(raw_row, sheet.book.datemode)

                # skip first row
                if 'date' in row[1].lower():
                    start_processing = True
                    header = row
                    continue

                if start_processing:
                    raw_item = {may_strip(head): row[idx] for idx, head in enumerate(header)}
                    # contextualise raw item with meta info
                    raw_item.update(
                        reported_date=reported_date,
                        provider_name=self.provider,
                        # source is for Fujairah, FOTT Terminal
                        port_name='Fujairah',
                        installation='Fujairah Oil Tanker Terminals',
                    )
                    yield from normalize.process_item(raw_item)

    @property
    def missing_rows(self):
        return normalize.MISSING_ROWS
