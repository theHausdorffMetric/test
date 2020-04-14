import datetime as dt

from dateutil.relativedelta import relativedelta
import xlrd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import decrypt, format_cells_in_row
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.seaforth_kenya import (
    normalize_consolidated,
    normalize_containerized,
    normalize_tanzania,
)
from kp_scrapers.spiders.bases.mail import MailSpider


class SeaforthKenyaSpider(ShipAgentMixin, MailSpider):
    name = 'SFK_Kenya'
    provider = 'Seaforth Kenya'
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
        Excel contains historical, daily runs should only retrieve the current month
        tab. Users may choose to rescrap historicals again. This is only for tanker
        statistics attachment
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
        for attachment in mail.attachments():
            if 'tanzania' in attachment.name.lower():
                yield from self.parse_tanzania(attachment, reported_date)

            if 'containerised' in attachment.name.lower():
                yield from self.parse_containerized(attachment, reported_date)

            if not any(sub in attachment.name.lower() for sub in ('containerised', 'tanzania')):
                if self.historical:
                    # historical sheet name
                    sheet_name = ['to date']
                else:
                    sheet_name = [
                        (dt.datetime.now() - relativedelta(months=1)).strftime('%B %Y').lower(),
                        dt.datetime.now().strftime('%B %Y').lower(),
                    ]
                yield from self.parse_consolidated(attachment, reported_date, sheet_name)

    # parse tanzania attachment
    def parse_tanzania(self, attachment, email_rpt_date):
        for sheet in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets():
            start_processing = False
            # store state of the table, in order to get relevant rows to extract
            for idx, raw_row in enumerate(sheet.get_rows()):
                # Include handling of xlrd.xldate.XLDateAmbiguous cases
                row = format_cells_in_row(raw_row, sheet.book.datemode)

                # skip irrelevant rows
                if 'vessel name' in row[0].lower():
                    start_processing = True
                    header = row
                    continue

                if start_processing:
                    raw_item = {
                        may_strip(head.lower()): row[idx] for idx, head in enumerate(header)
                    }
                    # contextualise raw item with meta info
                    raw_item.update(
                        reported_date=email_rpt_date,
                        provider_name=self.provider,
                        port_name='Dar Es Salam',
                    )
                    if DataTypes.CargoMovement in self.produces:
                        yield from normalize_tanzania.process_item(raw_item)

    # parse consolidated attachment
    def parse_consolidated(self, attachment, email_rpt_date, sheet_name):
        start_processing = False
        for sheet in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets():
            if sheet.name.lower() in sheet_name:
                # store state of the table, in order to get relevant rows to extract
                for idx, raw_row in enumerate(sheet.get_rows()):
                    # Include handling of xlrd.xldate.XLDateAmbiguous cases
                    row = format_cells_in_row(raw_row, sheet.book.datemode)

                    # skip irrelevant rows
                    if 'vessel' in row[0].lower():
                        start_processing = True
                        header = row
                        continue
                    if start_processing:
                        raw_item = {
                            may_strip(head.lower()): row[idx] for idx, head in enumerate(header)
                        }
                        # contextualise raw item with meta info
                        raw_item.update(reported_date=email_rpt_date, provider_name=self.provider)
                        if DataTypes.CargoMovement in self.produces:
                            yield from normalize_consolidated.process_item(raw_item)

    # parse containerized attachment
    def parse_containerized(self, attachment, email_rpt_date):
        workbook = decrypt(attachment.body, password='davidc')
        for sheet in workbook.sheets():
            start_processing = False
            if sheet.name.lower() in ('liquid bulk', 'dry Bulk', 'steel'):
                # store state of the table, in order to get relevant rows to extract
                for idx, raw_row in enumerate(sheet.get_rows()):
                    # Include handling of xlrd.xldate.XLDateAmbiguous cases
                    row = format_cells_in_row(raw_row, sheet.book.datemode)

                    # skip irrelevant rows
                    if 'vessel' in row[0].lower():
                        start_processing = True
                        header = row
                        continue

                    if start_processing:
                        raw_item = {
                            may_strip(head.lower()): row[idx] for idx, head in enumerate(header)
                        }
                        # contextualise raw item with meta info
                        raw_item.update(
                            reported_date=email_rpt_date,
                            provider_name=self.provider,
                            port_name='Mombasa',
                        )
                        if DataTypes.CargoMovement in self.produces:
                            yield normalize_containerized.process_item(raw_item)
