from collections import Counter
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.kanoo_djibouti import normalize
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider


class KanooSpider(ShipAgentMixin, PdfSpider, MailSpider):
    name = 'KN_Djibouti_Grades'
    provider = 'Kanoo'
    version = '1.0.0'
    produces = [DataTypes.Cargo, DataTypes.Vessel, DataTypes.CargoMovement]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    tabula_options = {'--guess': [], '--pages': ['all'], '--lattice': []}

    def parse_mail(self, mail):
        """Entry point of KN_SouthAfrica_Grades spider.

        Args:
            mail (Mail):

        Yields:
            Portcall:

        """
        self.reported_date = to_isoformat(mail.envelope['date'])

        for attachment in mail.attachments():
            # only parse vessels in port or ships schedule attachements
            if attachment.is_pdf and any(
                sub in attachment.name.lower() for sub in ('vessels in', 'schedule')
            ):
                reported_date = self.extract_reported_date(attachment.name, self.reported_date)
                yield from self.parse_pdf(attachment, reported_date)

    def parse_pdf(self, attachment, rpt_date):
        """Parse pdf table.

        Args:
            body (Body): attachment

        Yields:
            Dict[str, Any]
        """
        table = self.extract_pdf_io(attachment.body, **self.tabula_options)
        start_processing = False
        month_row = None

        for row in table:
            # to handle different attachments and tables within the attachment
            if 'OPERATIONS' in ''.join(row):
                header = row
                start_processing = True
                continue

            if start_processing:
                # remove empty vessel cells
                if not row[0]:
                    continue

                # a full row contains 11 cells, the month row would have 10 epmty cells given
                # that the first cell would contain the string required
                if Counter(row).get('') == 10:
                    month_row = row[0]

                raw_item = {header[idx]: cell for idx, cell in enumerate(row)}
                raw_item.update(
                    {
                        'reported_date': rpt_date,
                        'provider_name': self.provider,
                        'port_name': 'Djibouti',
                        'month_row': month_row,
                    }
                )
                yield from normalize.process_item(raw_item)

    @staticmethod
    def extract_reported_date(raw_rpt_string, email_date):
        """Extract reported date from attachment

        Args:
            body (Body): attachment name

        Returns:
            str:
        """
        rpt_match = re.match(r'.*?(\d{1,2}\s\d{1,2}\s\d{2,4})', raw_rpt_string)
        if rpt_match:
            return to_isoformat(rpt_match.group(1), dayfirst=True)
        return email_date
