from io import BytesIO
from zipfile import ZipFile

from dateutil.parser import parse as parse_date

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.kn_bahrain import normalize
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider


class KanooBahrainSpider(ShipAgentMixin, PdfSpider, MailSpider):
    name = 'KN_Bahrain_Grades'
    port = 'Sitra'
    provider = 'Kanoo'
    version = '1.0.1'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    tabula_options = {'--guess': [], '--lattice': []}

    reported_date = None

    def parse_mail(self, mail):
        """Entry point of KN_Bahrain_Grades spider.

        Args:
            mail (Mail):

        Returns:
            PortCall:

        """
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        for attachment in mail.attachments():
            # attachment can be in pdf or zipped together with other files
            if attachment.is_pdf:
                doc = attachment.body

            elif attachment.is_zip:
                # TODO: create a library to extract zip files
                z = ZipFile(BytesIO(attachment.body))
                locate_doc = next((name for name in z.namelist() if 'sitra' in name.lower()), None)
                if locate_doc is not None:
                    doc = z.read(locate_doc)
                else:
                    self.logger.warning('unable to find sitra file')
                    continue

            else:
                self.logger.warning(f'{attachment.name} discarded')
                continue

            header = None
            table = self.extract_pdf_io(doc, **self.tabula_options)
            for row in table:
                if self.is_header_row(row):
                    header = row
                    continue

                if header and self.is_relevant_row(header, row):
                    raw_item = {h: row[idx] for idx, h in enumerate(header) if h}
                    raw_item.update(self.meta_field)

                    yield from normalize.process_item(raw_item)

    @staticmethod
    def is_header_row(row):
        """Detect if it's header row.

        Args:
            row (List[str]):

        Returns:
            Boolean:

        """
        return 'Vessel Name' in row

    @staticmethod
    def is_relevant_row(header, row):
        """Detect if the row is relevant data row useful to Kpler.

        Args:
            header (List[str]):
            row (List[str]):

        Returns:
            Boolean:

        """
        mandatory_idx = header.index('Vessel Name')
        return len(header) == len(row) and row[mandatory_idx] and row[mandatory_idx] != '-'

    @property
    def meta_field(self):
        return {
            'provider_name': self.provider,
            'reported_date': self.reported_date,
            'port_name': self.port,
        }
