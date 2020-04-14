import xlrd

from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.samudra_india import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


HEADER_STR = [
    'vsl name',
    'vslname',
    'vessel',
    'vessel name',
    'vsls name',
    'name of vessel',
    'vessel (loa)',
    'name of the vessels',
]


class SamudraIndiaSpider(ShipAgentMixin, MailSpider):
    """Parse Liquid West/East Coast from Email

    This portion is maintained to process dry coast and serve
    as a backup for liquid

    """

    name = 'SD_India_Grades'
    version = '1.0.0'
    provider = 'Samudra'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]
    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """Parse each email that was matched with the spider filter arguments.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:
        """
        for attachment in mail.attachments():
            for sheet in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets():
                start_processing = False
                for idx, raw_row in enumerate(sheet.get_rows()):
                    row = format_cells_in_row(raw_row, sheet.book.datemode)
                    # detect if cell is a port cell and memoise it
                    if any(may_strip(x.lower()) in HEADER_STR for x in (row[0], row[1], row[3])):
                        header = row
                        start_processing = True
                        continue

                    if start_processing:
                        raw_item = {
                            may_strip(h.lower()): may_strip(row[idx].replace('\n', '|').lower())
                            for idx, h in enumerate(header)
                        }

                        # occasionally the header might be missing an important header
                        if '' in [key for key in raw_item.keys()]:
                            raw_item['eta_holder'] = raw_item.pop('', None)

                        raw_item.update(
                            provider_name=self.provider,
                            reported_date=mail.envelope['subject'],
                            port_name=may_strip(sheet.name.lower()),
                        )
                        yield from normalize.process_item(raw_item)

    @property
    def missing_rows(self):
        return normalize.MISSING_ROWS
