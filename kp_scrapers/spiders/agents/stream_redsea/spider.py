import xlrd

from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.stream_redsea import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


class StreamRedSeaSpider(ShipAgentMixin, MailSpider):

    name = 'STM_RedSea_Grades'
    provider = 'Stream Ships'
    version = '1.0.0'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """The method will be called for every mail the search_term matched.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:

        """
        # memoise and use received timestamp of email as default reported date
        reported_date_str = mail.envelope['subject']

        for attachment in mail.attachments():
            doc = attachment.body

            if 'red' in attachment.name.lower():
                accepted_docs = ['port', '1', '2', '5']
                header_rows = [7, 8]
                port_name_row = 1
                port_name = None

                yield from self.parse_attachment(
                    doc,
                    reported_date_str,
                    accepted_docs,
                    header_rows,
                    port_name_row,
                    port_name,
                    attachment.name,
                )

            if 'egypt' in attachment.name.lower():
                if 'liquefied natural gas' in attachment.name.lower():
                    accepted_docs = ['port', '1', '2', '5']
                    header_rows = [7, 8]
                    port_name_row = 1
                    port_name = None

                    yield from self.parse_attachment(
                        doc,
                        reported_date_str,
                        accepted_docs,
                        header_rows,
                        port_name_row,
                        port_name,
                        attachment.name,
                    )

            if any(sub in attachment.name.lower() for sub in ('liquefied petroleum gas', 'veg')):
                accepted_docs = ['port', '1', '2', '4', 'terminal']
                header_rows = [7, 8]
                port_name_row = 1
                port_name = None

                yield from self.parse_attachment(
                    doc,
                    reported_date_str,
                    accepted_docs,
                    header_rows,
                    port_name_row,
                    port_name,
                    attachment.name,
                )

            if 'adabiya' in attachment.name.lower():
                accepted_docs = ['status', 'arrived', 'expected', 'record']
                header_rows = [4, 5]
                port_name_row = None
                port_name = 'Adabiya'

                yield from self.parse_attachment(
                    doc,
                    reported_date_str,
                    accepted_docs,
                    header_rows,
                    port_name_row,
                    port_name,
                    attachment.name,
                )

    def parse_attachment(
        self, attachment_doc, reported_date, sheet_list, get_header, pn_row, pn, doc_name
    ):
        for sheet in xlrd.open_workbook(file_contents=attachment_doc, on_demand=True).sheets():
            # only parse relevant sheets
            if any(sub in sheet.name.lower() for sub in ['sheet1', 'new']) or not any(
                sub in sheet.name.lower() for sub in sheet_list
            ):
                continue

            first_row, second_row = None, None
            port_name = pn if pn else pn_row

            for idx, raw_row in enumerate(sheet.get_rows()):
                row = []
                # to handle is xldate exception
                for cell in raw_row:
                    if is_xldate(cell):
                        try:
                            cell = xldate_to_datetime(cell.value, sheet.book.datemode).isoformat()
                        except Exception:
                            cell = str(cell.value)

                    else:
                        cell = str(cell.value)

                    row.append(cell)

                # get port name
                if isinstance(port_name, int) and idx == pn_row:
                    port_name = row[1]

                # format headers because it is split into 2, make it adaptable
                # across sheets instead of hardcoding
                if idx in get_header:
                    if idx == get_header[0]:
                        first_row = row
                        continue

                    if idx == get_header[1]:
                        second_row = row
                        continue

                if first_row and second_row:
                    headers = self.combine_rows(first_row, second_row)

                # only process relevant rows after header row
                if idx > get_header[1]:
                    raw_item = {may_strip(head): row[idx] for idx, head in enumerate(headers)}
                    # contextualise raw item with meta info
                    raw_item.update(
                        reported_date=reported_date,
                        provider_name=self.provider,
                        port_name=port_name,
                        file_name=doc_name,
                    )
                    yield normalize.process_item(raw_item)

    @staticmethod
    def combine_rows(raw_row_1, raw_row_2):
        """Combine header rows

        Example Input:

        ['A', 'B', '', '', 'D']
        ['', '', 'E', 'F', '']

        Example Output:
        ['A', 'B', 'E', 'F', 'D']

        Args:
            raw_row_1 List[str]:
            raw_row_2 List[str]:

        Returns:
            List[str]: Combined header row

        """
        f_header = []

        for idx, item in enumerate(raw_row_1):
            if raw_row_1[idx] != '':
                f_header.append(raw_row_1[idx])
            elif raw_row_1[idx] == '':
                f_header.append(raw_row_2[idx])
            else:
                f_header.append('')
        return f_header
