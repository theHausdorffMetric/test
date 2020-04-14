import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.banchero_italy import (
    normalize_milazzo,
    normalize_napoli,
    normalize_panagia,
    normalize_savona_multedo,
    normalize_taranto,
)
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider


MILAZZO_HEADER = ['vessel', 'eta', 'etb', 'ets', 'berth', 'movement', 'product']
MISSING_ROWS = []


class BancheroItalySpider(ShipAgentMixin, PdfSpider, MailSpider):
    name = 'BCR_Italy_Grades'
    provider = 'Banchero'
    version = '1.0.0'
    produces = [DataTypes.Cargo, DataTypes.Vessel, DataTypes.CargoMovement]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    tabula_options = {'--guess': [], '--pages': ['all'], '--lattice': []}
    tabula_options_1 = {'--pages': ['all'], '--stream': []}

    def parse_mail(self, mail):
        """this email contains several attachment, the augusta attachment is an image
        and thus cannot be scraped. the function determines which attachment to scrap

        Args:
            mail (Mail):

        Yields:
            Portcall:

        """
        self.reported_date = to_isoformat(mail.envelope['date'])

        for attachment in mail.attachments():
            # parse taranto attachment
            if 'taranto' in attachment.name.lower():
                yield from self.parse_taranto_pdf(attachment, 'taranto', self.reported_date)

            # parse napoli attachment
            if 'napoli' in attachment.name.lower():
                yield from self.parse_napoli_pdf(attachment, 'naples', self.reported_date)

            # parse savona attachment
            if 'utf-8' in attachment.name.lower():
                yield from self.parse_savona_multedo_pdf(
                    attachment, 'Vado Ligure (Savona)', self.reported_date
                )

            # parse genoa attachment
            if any(sub in attachment.name.lower() for sub in ['genoa', 'multedo']):
                yield from self.parse_savona_multedo_pdf(attachment, 'Genoa', self.reported_date)

            # parse milazzo attachment
            if 'milazzo' in attachment.name.lower():
                yield from self.parse_milazzo_pdf(attachment, 'Milazzo', self.reported_date)

            # parse santa panagia attachment
            if 'panagia' in attachment.name.lower():
                yield from self.parse_panagia_pdf(attachment, 'Augusta', self.reported_date)

    def parse_napoli_pdf(self, attachment, port_name, email_rpt_date):
        """Parse pdf table.

        Args:
            body (Body): attachment

        Yields:
            Dict[str, Any]
        """
        table = self.extract_pdf_io(attachment.body, **self.tabula_options)
        start_processing = False

        for idx, row in enumerate(table):
            reported_date = email_rpt_date
            if idx == 0:
                try:
                    reported_date = to_isoformat(row[1], dayfirst=True)
                except Exception:
                    pass
                continue
            # to handle different attachments and tables within the attachment
            if 'Jetties' in row[0]:
                header = row
                start_processing = True
                continue

            if len(row) <= 5 or row[4] == 'Cargo' or not row[1]:
                continue

            if start_processing:
                raw_item = {header[idx]: cell for idx, cell in enumerate(row)}
                raw_item.update(
                    {
                        'reported_date': reported_date,
                        'provider_name': self.provider,
                        'port_name': port_name,
                    }
                )
                yield from normalize_napoli.process_item(raw_item)

    def parse_taranto_pdf(self, attachment, port_name, email_rpt_date):
        """Parse pdf table.

        Args:
            body (Body): attachment

        Yields:
            Dict[str, Any]
        """
        table = self.extract_pdf_io(attachment.body, **self.tabula_options)
        start_processing = False

        for idx, row in enumerate(table):
            if 'Situation' in row[0]:
                reported_date = to_isoformat(row[1], dayfirst=True)
                rpt_match = re.match(r'.*?(\d{1,2}\/\d{1,2}\/\d{2,4}).*', row[0])
                if rpt_match:
                    reported_date = to_isoformat(rpt_match.group(1), dayfirst=True)
            elif 'TARANTO\nBERTH SITUATION TANKER VESSEL' in ' '.join(row):
                reported_date = to_isoformat(row[0], dayfirst=True)
            else:
                reported_date = email_rpt_date

            # to handle different attachments and tables within the attachment
            if any(row[1] == sub for sub in ('VESSEL', 'MOTOR TANKER')):
                header = row
                header.pop(0)
                header.insert(-1, '')
                start_processing = True
                continue

            # to handle different attachments and tables within the attachment
            if any(row[0] == sub for sub in ('VESSEL', 'MOTOR TANKER')):
                header = row
                start_processing = True
                continue

            if start_processing and len(header) == len(row):
                raw_item = {header[idx]: cell for idx, cell in enumerate(row)}
                raw_item.update(
                    {
                        'reported_date': reported_date,
                        'provider_name': self.provider,
                        'port_name': port_name,
                    }
                )
                yield from normalize_taranto.process_item(raw_item)

    def parse_savona_multedo_pdf(self, attachment, port_name, email_rpt_date):
        """Parse pdf table.

        Args:
            body (Body): attachment

        Yields:
            Dict[str, Any]
        """
        table = self.extract_pdf_io(attachment.body, **self.tabula_options)
        start_processing = False

        for idx, row in enumerate(table):
            # to handle different attachments and tables within the attachment
            if 'STATUS' in row[1]:
                header = row
                start_processing = True
                continue

            if start_processing:
                raw_item = {header[idx]: cell for idx, cell in enumerate(row)}
                raw_item.update(
                    {
                        'reported_date': email_rpt_date,
                        'provider_name': self.provider,
                        'port_name': port_name,
                    }
                )
                yield from normalize_savona_multedo.process_item(raw_item)

    def parse_milazzo_pdf(self, attachment, port_name, email_rpt_date):
        """Parse pdf table.

        Args:
            body (Body): attachment

        Yields:
            Dict[str, Any]
        """
        # raw_table = self.extract_pdf_io(attachment.body, **self.tabula_options)
        table = self.extract_pdf_io(attachment.body, **self.tabula_options_1)
        start_processing = False
        r_pattern = r'^(.*)\s(\d{1,2}\/\d{1,2}\/\d{2,4}\s\d{1,2}:\d{1,2}|N\/A)\s(\d{1,2}\/\d{1,2}\/\d{2,4}\s\d{1,2}:\d{1,2}|N\/A)\s(\d{1,2}\/\d{1,2}\/\d{2,4}\s\d{1,2}:\d{1,2}|N\/A)(.*)\s(L|D| )(.*)$'  # noqa

        for idx, row in enumerate(table):
            # to handle different attachments and tables within the attachment
            if 'PORT NAME' in ' '.join(row):
                start_processing = True
                continue

            if start_processing:
                row = re.sub(r'\s+', ' ', ' '.join(row))
                row = self.split_row(row, r_pattern)

                if row and len(MILAZZO_HEADER) == len(row):
                    raw_item = {MILAZZO_HEADER[idx]: cell for idx, cell in enumerate(row)}
                    raw_item.update(
                        {
                            'reported_date': email_rpt_date,
                            'provider_name': self.provider,
                            'port_name': port_name,
                        }
                    )
                    yield from normalize_milazzo.process_item(raw_item)

    def parse_panagia_pdf(self, attachment, port_name, email_rpt_date):
        """Parse pdf table.

        Args:
            body (Body): attachment

        Yields:
            Dict[str, Any]
        """
        # raw_table = self.extract_pdf_io(attachment.body, **self.tabula_options)
        table = self.extract_pdf_io(attachment.body, **self.tabula_options_1)
        r_pattern = None

        for idx, row in enumerate(table):
            # to handle different attachments and tables within the attachment

            if 'Vessels At Berth' in ' '.join(row):
                headers = [
                    'berth',
                    'vessel',
                    'arrival',
                    'movement',
                    'volume',
                    'product',
                    'berthed',
                    'departure',
                ]
                r_pattern = r'^(.*?)\s(.*)\s([0-9]+hrs|[0-9]+\s[A-z]{2})\s(Load.|Load|Disch)\s([0-9]+)kt\s(.*?)\s([0-9]+hrs|[0-9]+\s[A-z]{2})\s([0-9]+hrs|[0-9]+\W[A-z]{2})$'  # noqa
                continue

            if any(sub in ' '.join(row) for sub in ('Vessels Anchored', 'Anchor')):
                headers = [
                    'vessel',
                    'arrival',
                    'movement',
                    'volume',
                    'product',
                    'berthed',
                    'departure',
                ]
                r_pattern = (
                    r'^(.*)\s(.*)\s(Load.|Load|Disch.|Disch)\s([0-9]+)kt\s(.*)\s(.*)\s(.*)$'  # noqa
                )
                continue

            if any(sub in ' '.join(row) for sub in ('Due To:', 'Eta')):
                headers = ['vessel', 'eta', 'movement', 'volume', 'product', 'berthed', 'departure']
                r_pattern = (
                    r'^(.*)\s(.*)\s(Load.|Load|Disch.|Disch)\s([0-9]+)kt\s(.*)\s(.*)\s(.*)$'  # noqa
                )
                continue

            if r_pattern:
                _row = self.split_row(re.sub(r'(\s+)', ' ', ' '.join(row)), r_pattern)

                if _row:
                    raw_item = {headers[idx]: cell for idx, cell in enumerate(_row)}
                    raw_item.update(
                        {
                            'reported_date': email_rpt_date,
                            'provider_name': self.provider,
                            'port_name': port_name,
                        }
                    )
                    yield normalize_panagia.process_item(raw_item)

                elif may_strip(re.sub(r'(\s+)', ' ', ' '.join(row))):
                    MISSING_ROWS.append(may_strip(re.sub(r'(\s+)', ' ', ' '.join(row))))

    @property
    def missing_rows(self):
        return MISSING_ROWS

    @staticmethod
    def split_row(row, regex_pattern):
        """Try to split the row.

        Args:
            row (str):

        Returns:
            Tuple(List[str], List[str]): cells and headers

        """
        _match = re.match(regex_pattern, row)  # noqa

        if _match:
            return _match.groups()
        return None
