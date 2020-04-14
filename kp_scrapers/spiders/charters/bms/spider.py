import xlrd

from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.bms import normalize_charters, normalize_grades


class BmsLineupSpider(CharterSpider, MailSpider):
    name = 'BMS'
    provider = 'BMS'
    version = '1.0.2'

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    # normalize data according to a schema
    normalize_data = None
    # filter to only process relevant attachments
    relevant_files = []

    def parse_mail(self, mail):
        """Extract mail found with specified email filters in spider arguments.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:

        """
        # get reported date from email header and memoise to avoid calling it repeatedly below
        reported_date = mail.envelope['date']

        for attachment in mail.attachments():
            # only process relevant attachments (denoted by their names)
            if not any(sheet in attachment.name.lower() for sheet in self.relevant_files):
                continue

            # some files have multiple sheets within them; extract all of them
            for sheet in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets():
                for raw_row in sheet.get_rows():
                    row = format_cells_in_row(raw_row, sheet.book.datemode)
                    # extract header row
                    # NOTE sometimes the header will not contain the "PORT" key where it should be
                    # "PORT key should be the first element in header
                    if 'VESSEL' in row:
                        header = row
                        header[0] = 'PORT'
                        header[5] = 'PRE. PORT'
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
                    )
                    if DataTypes.SpotCharter in self.produces:
                        yield from normalize_charters.process_item(raw_item)
                    # FIXME supposed to be `DataTypes.PortCall` here, but we don't want
                    # data-dispatcher to consume data from these spiders and the ETL to create PCs
                    else:
                        yield from normalize_grades.process_item(raw_item)


class BMSChartersCrudeSpider(BmsLineupSpider):
    name = 'BMS_Charters_Crude'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]
    relevant_files = ['crude', 'fuel']

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
        # prevent spider from marking the email as seen
        # so that multiple spiders can run on it
        'MARK_MAIL_AS_SEEN': False,
    }


class BMSChartersCleanSpider(BmsLineupSpider):
    name = 'BMS_Charters_Clean'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel, DataTypes.Cargo]
    relevant_files = ['product', 'ust-luga', 'primorsk']

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
        # already disable BMSGradesCleanSpider, no need to set it as False for now
        'MARK_MAIL_AS_SEEN': False,
    }


class BMSGradesCrudeSpider(BmsLineupSpider):
    name = 'BMS_Grades_Crude'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]
    relevant_files = ['crude', 'fuel']

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }


class BMSGradesCleanSpider(BmsLineupSpider):
    name = 'BMS_Grades_Clean'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]
    relevant_files = ['product', 'ust-luga', 'primorsk']

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
