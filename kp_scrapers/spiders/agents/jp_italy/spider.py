import re

import xlrd

from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.jp_italy import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


class JPReportItalySpider(ShipAgentMixin, MailSpider):
    name = 'JP_Italy'
    provider = 'JP Shipping'
    version = '1.0.0'

    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    # normalize data according to a schema
    normalize_data = None

    def parse_mail(self, mail):
        """Extract mail found with specified email filters in spider arguments.
        Args:
            mail (Mail):
        Yields:
            Dict[str, str]:
        """

        for attachment in mail.attachments():
            # get real reported date from attachment name
            date_match = re.search(r'(\d+(/|-)\d+(/|-)\d{2,4})', attachment.name)
            reported_date = date_match.group() if date_match else mail.envelope['date']
            # some files have multiple sheets within them; extract all of them
            for sheet in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets():
                # only visible sheets are required
                if sheet.visibility == 0:
                    for raw_row in sheet.get_rows():
                        row = format_cells_in_row(raw_row, sheet.book.datemode)
                        # extract header row
                        if 'Vessel Name' in row:
                            header = row
                            continue
                        # remove empty rows
                        if not row[0]:
                            continue

                        raw_item = {head: row[idx] for idx, head in enumerate(header)}
                        raw_item.update(
                            # we use the sheet tab name as the port name instead of listed port
                            provider_name=self.provider,
                            reported_date=reported_date,
                            port_name=may_strip(sheet.name),
                        )
                        yield from normalize.process_item(raw_item)
