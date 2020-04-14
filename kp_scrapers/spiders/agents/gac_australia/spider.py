import re
from typing import List, Optional, Tuple

from dateutil.parser import parse as parse_date
import xlrd

from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.gac_australia import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


SHEETS_TO_SKIP = ['SUMMARY', 'BULK DELAY REPORT']
SPECIAL_PORT_NAMES = ['GERALDTON', 'KWINANA', 'ESPERANCE']
DAMPIER_REPORT_PORTS = ['DAMPIER', 'WALCOTT']

PORT_MAPPING = {'DBCT': 'DALRYMPLE'}


class GACAustraliaSpider(ShipAgentMixin, MailSpider):
    name = 'GC_Australia'
    provider = 'GAC Shipping'
    version = '1.0.0'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """parse email
        Args:
            mail (Mail):
        Yields:
            Dict[str, str]:
        """
        reported_date_fallback = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        for attachment in (a for a in mail.attachments() if a.is_spreadsheet):
            reported_date = self.parse_reported_date(attachment.name) or reported_date_fallback
            for sheet in (
                s
                for s in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets()
                if s.name not in SHEETS_TO_SKIP
            ):
                yield from self.parse_raw_sheet(sheet, reported_date)

    def parse_raw_sheet(self, sheet, reported_date):
        """parse excel sheet
        Args:
            sheet (Sheet):
        Yields:
            Dict[str, str]
        """
        start_processing = False
        tmp_row = []
        # get product and the possible port name from the sheet name itself
        product, possible_ports = self.get_port_and_product(sheet.name)

        for idx, raw_row in enumerate(sheet.get_rows()):
            row = format_cells_in_row(raw_row, sheet.book.datemode)
            # to detect the start of the vessel table
            if len(row) > 0 and row[0].lower() == 'vessel':
                headers = row
                start_processing = True
                port_name, installation = self.detect_installation_and_port(tmp_row, possible_ports)

                # push the first element to the last. To handle sheets having multiple vessel table.
                if possible_ports and port_name != possible_ports[0]:
                    possible_ports = possible_ports[1:] + possible_ports[:1]

                continue

            if start_processing:
                raw_item = {h.lower(): row[idx] for idx, h in enumerate(headers)}

                raw_item.update(
                    provider_name=self.provider,
                    reported_date=reported_date,
                    port_name=port_name,
                    cargo_product=product,
                    installation=installation,
                )
                yield normalize.process_item(raw_item)

            # memoizing the last row scrapped
            tmp_row.append(row)

    @staticmethod
    def parse_reported_date(raw_reported_date: str) -> str:
        """Normalize raw reported date to a valid format string.
        """
        date_match = re.match(r'.*?(\d{1,2}\-\d{1,2}\-\d{1,4}).*', raw_reported_date)
        if date_match:
            return parse_date(date_match.group(1), dayfirst=True).strftime('%d %b %Y')

    @staticmethod
    def get_port_and_product(sheet_name: str) -> Tuple[Optional[str], Optional[List[str]]]:
        """From the sheet name retrieves the list of product and port names

        Examples:
            >>> GACAustraliaSpider.get_port_and_product('COAL - DAMPIER')
            ('COAL', ['DAMPIER', 'WALCOTT'])
            >>> GACAustraliaSpider.get_port_and_product('IRON ORE - ABBOT POINT')
            ('IRON ORE', ['ABBOT POINT'])

        """
        if sheet_name.endswith(' - DAMPIER'):
            return (sheet_name.split(' - ')[0].strip(), DAMPIER_REPORT_PORTS)

        try:
            product, port = sheet_name.split('-')
            return (
                product.strip(),
                [PORT_MAPPING.get(i.strip(), i.strip()) for i in port.split('&')],
            )
        except ValueError:
            if sheet_name == 'GERALDTON, KWINANA & ESPERANCE':
                return ('Iron Ore', SPECIAL_PORT_NAMES)

        return None, None

    @staticmethod
    def detect_installation_and_port(
        rows: List[str], ports: List[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """Grab the installation/port from the vessel table header

        Example 1

                                    Hay Point
        -------------------------------------------------------------------------
        Vessel | status | ETA |... | ... | ...|....

        Example 2:

        PWCS KOORAGANG COAL TERMINAL
        -------------------------------------------------------------------------
        Vessel | status | ETA |... | ....| ...|....

        Example 1 - Header denotes the Port Name
        Example 2 - Header denotes the installation

        """

        if not (ports and rows):
            return None, None

        if len(ports) > 1:
            # number of lines to traverse back.
            offset = 1
            potential_port_name = [
                ''.join(row) for row in rows[(len(rows) - 1) - offset :] if ''.join(row)
            ]
            for l in (p for p in ports for potPort in potential_port_name if p in potPort):
                return l, None

            return ports[0], None
        else:
            potential_port_name = [''.join(row) for row in rows[(len(rows) - 1) :] if ''.join(row)]
            return ports[0], potential_port_name.pop() if potential_port_name else None
