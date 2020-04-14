from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta
import xlrd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.benline_vietnam import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


class BenlineVietnamSpider(ShipAgentMixin, MailSpider):
    name = 'BL_Vietnam'
    provider = 'Ben Line'
    version = '1.0.0'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def __init__(self, historical=False, *args, **kwargs):
        """
        Excel may contain historical data, daily runs should only retrieve the current
        month tab or if the attachment only has a single sheet. Users may choose to
        rescrap historicals again.

        Args:
            historical (bool):
        """
        super().__init__(*args, **kwargs)
        self.historical = historical

    def parse_mail(self, mail):
        """Extract data from each mail matched by the query spider argument.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:

        """
        # memoise reported_date so it won't need to be called repeatedly later
        reported_date = to_isoformat(mail.envelope['date'])

        if not self.historical:
            # in case report is sent late
            sheet_name = [
                parse_date(reported_date).strftime('%b').lower(),
                (parse_date(reported_date) + relativedelta(months=-1)).strftime('%b').lower(),
            ]
            for attachment in mail.attachments():
                xlrd_obj = xlrd.open_workbook(file_contents=attachment.body, on_demand=True)
                for sheet in xlrd_obj.sheets():
                    if sheet.name.lower() in sheet_name or len(xlrd_obj.sheet_names()) == 1:
                        yield from self.parse_attachment(attachment, reported_date, sheet)

        else:
            for attachment in mail.attachments():
                for sheet in xlrd.open_workbook(
                    file_contents=attachment.body, on_demand=True
                ).sheets():
                    yield from self.parse_attachment(attachment, reported_date, sheet)

    def parse_attachment(self, attachment, email_rpt_date, raw_sheet):
        """Extract data from each mail matched by the query spider argument.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:

        """
        start_processing = False
        for idx, raw_row in enumerate(raw_sheet.get_rows()):
            # Include handling of xlrd.xldate.XLDateAmbiguous cases
            row = format_cells_in_row(raw_row, raw_sheet.book.datemode)

            # skip irrelevant rows
            if 'date' in row[0].lower():
                header = row
                start_processing = True
                continue

            if start_processing:
                raw_item = {may_strip(head.lower()): row[idx] for idx, head in enumerate(header)}
                # contextualise raw item with meta info
                raw_item.update(reported_date=email_rpt_date, provider_name=self.provider)

                yield normalize.process_item(raw_item)
