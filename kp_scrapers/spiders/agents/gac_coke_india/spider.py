import xlrd

from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.gac_coke_india import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


class GACIndiaSpider(ShipAgentMixin, MailSpider):
    name = 'GC_IndiaCoke_Grades'
    provider = 'GAC Shipping'
    version = '1.0.0'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """parse email
        Args:
            mail (Mail):
        Yields:
            Dict[str, str]:
        """
        for attachment in mail.attachments():
            for sheet in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets():
                start_processing = False
                reported_date = None
                port = None
                for idx, raw_row in enumerate(sheet.get_rows()):
                    row = format_cells_in_row(raw_row, sheet.book.datemode)
                    if 'coal / coke vessel line up' in ''.join(row).lower():
                        reported_date = may_strip(''.join(row).lower())
                        continue

                    # detect if cell is a port cell and memoise it
                    if 'position' in row[0].lower():
                        headers = row
                        start_processing = True
                        continue

                    if start_processing:
                        # determine port row
                        if row.count('') >= 10:
                            port = row[0]
                            continue

                        raw_item = {h.lower(): row[idx] for idx, h in enumerate(headers)}
                        raw_item.update(
                            provider_name=self.provider, port_name=port, reported_date=reported_date
                        )
                        yield normalize.process_item(raw_item)
