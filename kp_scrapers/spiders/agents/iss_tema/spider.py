import re

import xlrd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.iss_tema import normalize, parser
from kp_scrapers.spiders.bases.mail import MailSpider


RELEVANT_NAMES = ['port', 'anchorage', 'expected', 'permanent vesls', 'wrecked', 'beached vessel']


class ISSTemaSpider(ShipAgentMixin, MailSpider):
    name = 'ISS_Tema_Grades'
    provider = 'ISS'
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
        reported_date = self.extract_reported_date(mail.envelope['subject'])

        for attachment in mail.attachments():
            if not attachment.is_spreadsheet:
                continue

            for sheet in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets():
                if sheet.name.lower() not in RELEVANT_NAMES:
                    continue

                # store state of the table, in order to get relevant rows to extract
                for idx, raw_row in enumerate(sheet.get_rows()):
                    row = []
                    # to handle is xldate exception
                    for cell in raw_row:
                        if is_xldate(cell):
                            try:
                                cell = xldate_to_datetime(
                                    cell.value, sheet.book.datemode
                                ).isoformat()
                            except Exception:
                                cell = str(cell.value)

                        else:
                            cell = str(cell.value)

                        row.append(cell)

                    # retrieve static information
                    _static_info = parser.HEADER_SHEET_MAPPING.get(sheet.name.lower())

                    # ignore unnecessary rows before header row
                    if idx < _static_info[0]:
                        continue

                    header = _static_info[2]

                    # ignore rows where vessel column is empty
                    # can be put in normalize but this file has a lot of empty rows
                    # at the end of the report. Putting it here would cut the noise
                    if not row[_static_info[1]]:
                        continue

                    raw_item = {head: row[idx] for idx, head in enumerate(header)}
                    # contextualise raw item with meta info
                    raw_item.update(reported_date=reported_date, provider_name=self.provider)
                    yield from normalize.process_item(raw_item)

    @staticmethod
    def extract_reported_date(rpt_date):
        """Normalize raw reported date to a valid format string.

        Examples - Fwd: Tema Port State : 01 April 2019

        Args:
            raw_date (str)

        Returns:
            str | None:

        """
        _match = re.match(r'.*\s:\s(\d{1,2}\s[A-z]+\s\d{2,4})', rpt_date)
        if _match:
            return to_isoformat(_match.group(1), dayfirst=True)
