import xlrd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.sgs_grades import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


class SGSGradesSpider(ShipAgentMixin, MailSpider):
    name = 'SGS_Grades'
    provider = 'SGS'
    version = '1.1.1'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """This method will be called for every mail the search_term matched.

        Each vessel movement has an associated uuid that is linked to a cargo's uuid,
        allowing for easy retrieval of a vessel's cargo movement.

        However, each vessel may contain multiple cargo movements with the same uuid,
        therefore we store the products as a list value against the uuid key.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:
        """
        # memoise reported date so it does not need to be called repeatedly later
        reported_date = to_isoformat(mail.envelope['date'])

        for attachment in mail.attachments():
            # sanity check in case file is not a spreadsheet
            if not attachment.is_spreadsheet:
                continue

            # memoise list of all cargoes first
            cargoes = {}
            for product in self.parse_sheet_rows(
                xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheet_by_name(
                    'Products'
                )
            ):
                if cargoes.get(product['ExportId']):
                    cargoes[product['ExportId']].append(product)
                else:
                    cargoes[product['ExportId']] = [product]

            # get all vessel movements (without cargoes)
            for raw_item in self.parse_sheet_rows(
                xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheet_by_name(
                    'PortTraffic'
                )
            ):
                # iterate across all products in the onboard cargo
                # cargoes are matched to their portcalls with their UUIDs
                for cargo in cargoes.get(raw_item['id'], []):
                    # let's add the product data, and some meta info
                    raw_item.update(
                        provider_name=self.provider, reported_date=reported_date, **cargo
                    )
                    yield normalize.process_item(raw_item)

    def parse_sheet_rows(self, sheet, post_process=lambda x: x):
        """Parse and process rows of a spreadsheet.

        Args:
            sheet (xlrd.Sheet):
            post_process (Callable[Any, List[str]]):

        Yields:
            Dict[str, str]:
        """
        for idx, row in enumerate(sheet.get_rows()):
            row = [
                xldate_to_datetime(cell.value, sheet.book.datemode).isoformat()
                if is_xldate(cell)
                else may_strip(cell.value)
                for cell in row
            ]

            # headers will always be the first row of any sheet from this source
            if idx == 0:
                header = row
                continue

            # post-process row according to caller specifications
            row = post_process(row)
            if row:
                yield {head: row[head_idx] for head_idx, head in enumerate(header)}
