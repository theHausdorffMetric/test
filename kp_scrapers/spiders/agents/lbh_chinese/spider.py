import xlrd

from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.lbh_chinese import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


class LBHChineseSpider(ShipAgentMixin, MailSpider):
    name = 'LBH_Chinese_Grades'
    provider = 'LBH Shipping'
    version = '1.0.1'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """parse 2 email attachments iron and coal for australia
        Args:
            mail (Mail):
        Yields:
            Dict[str, str]:
        """
        for attachment in mail.attachments():
            for sheet in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets():
                # to prevent the processing of the summary and useless sheets
                if sheet.name not in ['汇总', 'Sheet1', 'Sheet2']:
                    for idx, raw_row in enumerate(sheet.get_rows()):
                        row = format_cells_in_row(raw_row, sheet.book.datemode)

                        # detect if cell is a port cell and memoise it
                        if 'vessel' in row[2].lower():
                            headers = row
                            continue

                        raw_item = {h.lower(): row[idx] for idx, h in enumerate(headers)}
                        raw_item.update(provider_name=self.provider, port_name=sheet.name)
                        yield normalize.process_item(raw_item)
