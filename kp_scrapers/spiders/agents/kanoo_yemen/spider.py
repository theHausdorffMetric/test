import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.kanoo_yemen import normalize
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider


class KanooSpider(ShipAgentMixin, PdfSpider, MailSpider):
    name = 'KN_Yemen_Grades'
    provider = 'Kanoo'
    version = '1.0.0'
    produces = [DataTypes.Cargo, DataTypes.Vessel, DataTypes.CargoMovement]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
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
            if attachment.is_pdf:
                if 'hodeidah' in attachment.name.lower():
                    a_port_name = 'Hodeidah'
                    yield from self.parse_pdf(attachment, a_port_name)
                if 'saleef' in attachment.name.lower():
                    a_port_name = 'Saleef'
                    yield from self.parse_pdf(attachment, a_port_name)

    def parse_pdf(self, attachment, attachement_port_name):
        """Parse pdf table.

        Args:
            body (Body): attachment

        Yields:
            Dict[str, Any]
        """
        table = self.extract_pdf_io(attachment.body, **self.tabula_options)

        # get the port_name from the filename
        port_name = attachement_port_name
        for row in table:
            if 'VESSEL' in row[0]:
                header = row
                # arrival and berthed cols are split into 2, date and time
                insert_dt_list = self.detect_header(header)
                for number in insert_dt_list:
                    header.insert(number + 1, '{}_time'.format(header[number]))

                continue

            # remove empty vessel cells
            if not row[0]:
                continue

            # extra spaces may be found at the end of rows
            while len(row) != len(header):
                row.insert(len(row), '')

            raw_item = {header[idx]: cell for idx, cell in enumerate(row)}
            raw_item.update(
                {
                    'reported_date': self.reported_date,
                    'provider_name': self.provider,
                    'port_name': port_name,
                }
            )
            yield from normalize.process_item(raw_item)

    @staticmethod
    def detect_header(header_list):
        """detect date andtime headers to insert extra columns

        Example header and output:
        ["VESSEL'S NAME", 'LOA', 'DRAFT', 'CARGO', 'QTY', 'ARRIVED', 'BERTHED', 'B. NO.', 'REMARKS/ETC', '', ''] # noqa

        List returned [6, 5]

        Args:
            header_list (List):

        Returns:
            List[str]:
        """
        PATTERNS = ['arrived', 'berthed', 'e.t.a', 'e.t.b']
        time_index_list = []
        for h_idx, val in enumerate(header_list):
            for pattern in PATTERNS:
                if re.search(pattern, val.lower()):
                    time_index_list.append(h_idx)

        return time_index_list[::-1]
