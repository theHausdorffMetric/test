import xlrd

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.itl_indonesia import normalize, normalize_movement
from kp_scrapers.spiders.bases.mail import MailSpider


class ItlIndonesiaSpider(ShipAgentMixin, MailSpider):
    name = 'ITL_Indonesia'
    provider = 'ITL'
    version = '2.0.0'

    def parse_mail(self, mail):
        """Extract mail found with specified email filters in spider arguments.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:
        """
        for attachment in mail.attachments():
            # there is only one sheet within the file
            self.sheet = xlrd.open_workbook(
                file_contents=attachment.body, on_demand=True
            ).sheet_by_index(0)

            is_relevant_data = False
            for idx, row in enumerate(self.sheet.get_rows()):
                # skip useless header rows
                if not is_relevant_data:
                    if 'DateTime' in ''.join([cell.value for cell in row]):
                        is_relevant_data = True
                        continue
                    else:
                        continue

                raw_item = {str(cell_idx): cell.value for cell_idx, cell in enumerate(row)}
                raw_item.update(
                    provider_name=self.provider,
                    reported_date=mail.envelope['date'],
                    sheet_mode=self.sheet.book.datemode,
                )
                if DataTypes.PortCall in self.produces:
                    yield from normalize.process_item(raw_item)
                if DataTypes.Cargo in self.produces:
                    yield normalize_movement.process_item(raw_item)


class ITLIndonesiaPC(ItlIndonesiaSpider):
    name = 'ITL_Indonesia_PC'
    # we want to create portcalls from this commercial report, hence PortCall model is used
    produces = [DataTypes.PortCall]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
        # prevent spider from marking the email as seen
        # so that multiple spiders can run on it
        'MARK_MAIL_AS_SEEN': False,
    }


class ITLIndonesiaMovement(ItlIndonesiaSpider):
    name = 'ITL_Indonesia_Movements'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
