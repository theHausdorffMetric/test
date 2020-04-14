import xlrd

from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.deepblue_colombia import normalize_charters, normalize_grades
from kp_scrapers.spiders.bases.mail import MailSpider


class DeepBlueLineupSpider(ShipAgentMixin, MailSpider):
    name = 'DB_Colombia'
    provider = 'Deep Blue Ship Agency'
    version = '1.0.0'

    # normalize data according to a schema
    normalize_data = None

    def parse_mail(self, mail):
        """Extract mail found with specified email filters in spider arguments.
        Args:
            mail (Mail):
        Yields:
            Dict[str, str]:
        """
        # get hypothetical reported date from email header
        reported_date = mail.envelope['date']

        for attachment in mail.attachments():
            # some files have multiple sheets within them; extract all of them
            for sheet in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets():
                # only visible sheets are required
                if sheet.visibility == 0:
                    for raw_row in sheet.get_rows():
                        row = format_cells_in_row(raw_row, sheet.book.datemode)
                        # extract header row
                        if 'VESSEL' in row:
                            header = row
                            continue
                        # extract real reported date
                        if 'Date:' in row:
                            reported_date = row[2]
                            continue
                        # remove empty rows and rows with "NIL" vessels
                        if not row[3] or row[3] in ['NIL', 'TBN']:
                            continue
                        raw_item = {head: row[idx] for idx, head in enumerate(header)}
                        raw_item.update(
                            # we use the sheet tab name as the port name instead of listed port
                            region_name=sheet.name,
                            provider_name=self.provider,
                            reported_date=reported_date,
                            spider_name=self.name,
                            port_name=may_strip(sheet.name.lower()),
                        )
                        if DataTypes.SpotCharter in self.produces:
                            yield from normalize_charters.process_item(raw_item)
                            # FIXME supposed to be `DataTypes.PortCall` here, but we don't want
                            # data-dispatcher to consume data from these spiders and the ETL
                            # to create PCs
                        elif DataTypes.Cargo in self.produces:
                            yield from normalize_grades.process_item(raw_item)


class DeepBlueChartersSpider(DeepBlueLineupSpider):
    name = 'DB_Colombia_Charters'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
        # prevent spider from marking the email as seen
        # so that multiple spiders can run on it
        'MARK_MAIL_AS_SEEN': False,
    }


class DeepBlueGradesSpider(DeepBlueLineupSpider):
    name = 'DB_Colombia_Grades'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
