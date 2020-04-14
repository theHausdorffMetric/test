import logging
import os
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.platts import normalize, normalize_lineups, parser


logger = logging.getLogger(__name__)


MISSING_ROWS = []


LINE_UP_HEADERS = [
    'NO.',
    'VESSELS NAME',
    'QUANTITY',
    'ETA',
    'LOAD',
    'ETC / D',
    'DESTINATION',
    'SHIPPER',
]


class IDTSpider(PdfSpider, MailSpider):
    version = '2.0.0'

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    tabula_platts_options = {'--guess': [], '--pages': ['all'], '--stream': []}

    def parse_mail(self, mail):
        """Entry point of IDT_PTDry_Fixtures spider.

        Args:
            mail (Mail):

        Yields:
            SpotCharter:

        """
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        for attachment in mail.attachments():
            # A single email contains a lot of attachments.
            # The file starting with ICT is used, as it is the only file containing valid info.
            if re.search(r'ICT_*', attachment.name) and DataTypes.SpotCharter in self.produces:
                if attachment.is_pdf:
                    yield from self.parse_platts_pdf(attachment)

            if (
                re.search(r'DAILY LINE UP*', attachment.name)
                and DataTypes.CargoMovement in self.produces
            ):
                if attachment.is_pdf:
                    yield from self.parse_idt_lineup_pdf(attachment)

    def parse_platts_pdf(self, attachment):
        """Parse pdf table.

        Args:
            body (Body): attachment

        Yields:
            Dict[str, Any]

        """
        table = self.extract_pdf_io(attachment.body, **self.tabula_platts_options)

        PROCESSING_STARTED = False
        for row in table:
            row_values = [may_strip(val).lower() for val in row if val]
            if 'vessel' in row_values:  # an indicator to identify the valid table starting
                header_row = [may_strip(val).lower() for val in row if val]
                PROCESSING_STARTED = True
                continue

            if PROCESSING_STARTED:
                if len(row) == len(header_row):  # to identify the correct table
                    table_row = [may_strip(val) for val in row if val]
                    if len(table_row) == len(header_row):  # to ignore records with just country
                        raw_item = dict(zip(header_row, table_row))
                        raw_item.update(
                            {'reported_date': self.reported_date, 'provider_name': 'Platts'}
                        )
                        yield normalize.process_item(raw_item)

    def parse_idt_lineup_pdf(self, attachment):
        """Parse pdf table.

        Args:
            body (Body): attachment

        Yields:
            Dict[str, Any]

        """
        # for consistent parsing, pdf_to_text is used instead of tabula
        self.save_file(attachment.name, attachment.body)
        text = PdfSpider.pdf_to_text(filepath=os.path.join(self.data_path, attachment.name))
        port_name = None
        prev_row = None

        for line in text.split('\n'):
            if 'VESSELS NAME' in ''.join(line):
                port_name = prev_row
                continue

            prev_row = ''.join(line)
            row = parser.validate_list(line)

            # append malformed rows to notify analyst
            if len(row) == 7:
                MISSING_ROWS.append(' '.join(row))

            # discard useless rows
            if not row or len(row) != len(LINE_UP_HEADERS):
                continue

            raw_item = {header: row[idx] for idx, header in enumerate(LINE_UP_HEADERS)}
            raw_item.update(
                {
                    'reported_date': self.reported_date,
                    'provider_name': 'IDT',
                    'port_name': port_name,
                }
            )
            yield normalize_lineups.process_item(raw_item)

    @property
    def missing_rows(self):
        """So that analysts will be notified."""
        return MISSING_ROWS


class IDTFixturesSpider(CharterSpider, IDTSpider):
    name = 'IDT_PTDry_Fixtures'
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


class IDTGradesSpider(ShipAgentMixin, IDTSpider):
    name = 'IDT_PTDry_PlayerGrades'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
