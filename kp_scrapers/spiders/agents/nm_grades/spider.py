import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.nm_grades import normalize
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider


class NextMaritimeSpider(ShipAgentMixin, PdfSpider, MailSpider):
    name = 'NM_Grades'
    provider = 'Next Maritime'
    version = '1.0.1'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    tabula_options = {'--stream': [], '--area': ['114.5,30,548,850'], '--pages': ['all']}
    # only area and pages parameters are enough to scrap the reported date correctly
    # to get the exact coordinates for area, it is best to use tabula plugin.
    tabula_reported_date_options = {'--area': ['98.214,26.337,111.383,91.081'], '--pages': ['1']}

    def parse_mail(self, mail):
        current_date = to_isoformat(mail.envelope['date'])

        for attachment in mail.attachments():
            if not attachment.is_pdf:
                continue

            table = self.extract_pdf_io(attachment.body, **self.tabula_options)
            # parse reported date from the sheet name
            date_extracted = self.extract_pdf_io(
                attachment.body, **self.tabula_reported_date_options
            )

            # extract the reported_date if present, else use the current date as reported_date
            reported_date = self._retreive_reported_date(date_extracted)
            if not reported_date:
                reported_date = current_date

            for row in table:
                # extract table headers
                if 'COUNTRY' in row[0]:
                    header = row
                    continue

                if len(header) == len(row):
                    raw_item = {head: row[idx] for idx, head in enumerate(header)}
                    raw_item.update(provider_name=self.provider, reported_date=reported_date)

                    yield from normalize.process_item(raw_item)

    @staticmethod
    def parse_reported_date(raw_reported_date):
        """Normalize raw reported date to a valid format string.

        Args:
            raw_date (str)

        Returns:
            str | None:

        """
        date_match = re.match(r'\d{2}\/\d{2}\/\d{2,4}', raw_reported_date)
        if date_match:
            return parse_date(date_match.group(0), dayfirst=True).isoformat()

    def _retreive_reported_date(self, date_extracted):
        """Iterates through the date_extracted field to get the reported_date

        Iterates through the date_extracted field and if it is not None and not empty
        will parse the date to standard format
        """
        # sanity check; in the event tabula does not return expected output
        if not date_extracted:
            return None

        # the value is always present in the index 0
        return self.parse_reported_date(date_extracted[0][0]) if date_extracted[0] else None
