import re

from dateutil.parser import parse as parse_date
import xlrd

from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.jurfmar_libya import normalize_charters, normalize_grades


IRRELEVANT_SHEET_NAME = ['ports sitrep', 'port sitrep', 'ports status', 'port status']
# columns index where vessel name will reside in
VESSEL_COL_IDX = 1


class JFLibyaSpider(CharterSpider, MailSpider):

    name = 'JF_Libya'
    provider = 'Jurfmar'
    version = '1.1.1'

    def parse_mail(self, mail):
        """The method will be called for every mail the search_term matched.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:

        """
        # memoise and use received timestamp of email as default reported date
        reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        for attachment in mail.attachments():
            for sheet in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets():
                # only process the sheet we want
                if sheet.name.lower() in IRRELEVANT_SHEET_NAME:
                    continue

                # take reported date in sheet name if it contains one, else use default
                reported_date = self.parse_reported_date(attachment.name) or reported_date

                for idx, raw_row in enumerate(sheet.get_rows()):
                    row = format_cells_in_row(raw_row, sheet.book.datemode)
                    # first row is useless, discard it
                    if idx == 0:
                        continue

                    # second row will always contain header; extract it
                    if idx == 1:
                        header = row
                        continue

                    # extract data row
                    if row[VESSEL_COL_IDX] and row[VESSEL_COL_IDX] != 'TBN':
                        raw_item = {head: row[head_idx] for head_idx, head in enumerate(header)}
                        raw_item.update(reported_date=reported_date, provider_name=self.provider)
                        if DataTypes.SpotCharter in self.produces:
                            yield normalize_charters.process_item(raw_item)
                        # FIXME supposed to be `DataTypes.PortCall` here, but we don't want
                        # data-dispatcher to consume data from these spiders
                        # and the ETL to create PCs
                        else:
                            yield from normalize_grades.process_item(raw_item)

    @staticmethod
    def parse_reported_date(raw_reported_date):
        """Normalize raw reported date to a valid format string.

        FIXME charters loader assumes `dayfirst=True` when parsing, so we can't use ISO-8601

        Args:
            raw_date (str)

        Returns:
            str | None:

        """
        date_match = re.match(r'.*REPORT\s(.*)\.xlsx', raw_reported_date)
        if date_match:
            return parse_date(date_match.group(1), dayfirst=True).strftime('%d %b %Y')


class JFLibyaChartersSpider(JFLibyaSpider):
    name = 'JF_Libya_Charters'
    produces = [DataTypes.SpotCharter]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
        # prevent spider from marking the email as seen
        # so that multiple spiders can run on it
        'MARK_MAIL_AS_SEEN': False,
    }


class JFLibyaGradesSpider(JFLibyaSpider):
    name = 'JF_Libya_Grades'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
