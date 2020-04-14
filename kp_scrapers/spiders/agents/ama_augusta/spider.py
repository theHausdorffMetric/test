from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.ama_augusta import normalize
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider


class AMAAugustaSpider(ShipAgentMixin, PdfSpider, MailSpider):
    name = 'AMA_Augusta_Grades'
    provider = 'Avvisatore'
    version = '1.0.0'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    tabula_options = {'--guess': [], '--lattice': []}

    # source only provides data for this specific port, and nowhere else
    port_name = 'Augusta'

    def parse_mail(self, mail):
        # memoise reported date so it won't need to be computed repeatedly later on
        reported_date = to_isoformat(mail.envelope['date'])

        for attachment in mail.attachments():
            # sanity check, in case non-pdf files are sent
            if not attachment.is_pdf:
                continue

            table = self.extract_pdf_io(attachment.body, **self.tabula_options)

            for row in table:
                # detect relevant row info and header
                if self.is_date(row[0]) or 'Data' in row[0]:
                    # detect header row
                    if 'Data' in row[0]:
                        header = row
                        continue

                    if len(row) == len(header):
                        raw_item = {header[idx]: cell for idx, cell in enumerate(row)}
                        raw_item.update(
                            port_name=self.port_name,
                            provider_name=self.provider,
                            reported_date=reported_date,
                        )
                        yield normalize.process_item(raw_item)

    @staticmethod
    def is_date(string, fuzzy=False):
        """
        Return whether the string can be interpreted as a date.

        Args:
            string (str):
            fuzzy (bool): ignore unknown tokens in string if True

        Returns:
            bool:
        """
        try:
            parse_date(string)
            return True

        except ValueError:
            return False
