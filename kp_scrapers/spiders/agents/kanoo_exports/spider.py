import xlrd

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.kanoo_exports import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


# mandatory vessel name, or we consider it's an invalid row
VESSEL_IDX = 1

# these fields might be empty
PORT_IDX = 0
DATE_INDEXES = [9, 10, 11]


class KanooExportsSpider(ShipAgentMixin, MailSpider):
    name = 'KN_ExportVesselsStatement'
    provider = 'Kanoo'
    version = '1.0.0'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    reported_date = None

    def parse_mail(self, mail):
        """Entry point of mail parsing.

        Args:
            mail (Mail):

        Returns:
            PortCall:

        """
        self.reported_date = mail.envelope['date']
        for attachment in mail.attachments():
            sheet = xlrd.open_workbook(
                file_contents=attachment.body, on_demand=True
            ).sheet_by_index(0)

            processing_started = False
            _prev_row = None
            for raw_row in sheet.get_rows():
                row = [cell.value for cell in raw_row]

                # after header row, we start process
                if not processing_started and 'VESSELS NAME' in row:
                    processing_started = True
                    continue

                # vessel to identify empty row
                if not row[VESSEL_IDX]:
                    continue

                if processing_started:
                    # some rows might lack port name, use the previous one
                    row[PORT_IDX] = may_strip(row[PORT_IDX]) or _prev_row[PORT_IDX]

                    # date might be missing, if the vessel name is the same as previous row,
                    # use previous row's date as reference
                    if _prev_row and row[VESSEL_IDX] == _prev_row[VESSEL_IDX]:
                        for idx in DATE_INDEXES:
                            row[idx] = may_strip(row[idx]) or _prev_row[idx]
                    _prev_row = row

                    raw_item = {str(idx): cell for idx, cell in enumerate(row)}
                    raw_item.update(self.meta_field)

                    yield normalize.process_item(raw_item)

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
