import xlrd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.seoul_line import normalize_charters, normalize_grades


class SeoulLineSpider(CharterSpider, MailSpider):
    name = 'SEL'
    provider = 'Seoul Line'
    version = '1.0.1'

    def parse_mail(self, mail):
        """Extract mail found with specified email filters in spider arguments.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:
        """
        self.reported_date = to_isoformat(mail.envelope['date'])
        start_process = False

        for attachment in mail.attachments():
            for sheet in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets():
                if 'line-up' in sheet.name.lower():
                    for raw_row in sheet.get_rows():
                        row = format_cells_in_row(raw_row, sheet.book.datemode)

                        # extract header row
                        if 'VESSEL' in row[0].upper():
                            header = row
                            start_process = True
                            continue

                        if 'LIST' in row[0].upper():
                            year = row[0].partition('(')[-1].partition(')')[0]
                            start_process = False
                            continue

                        if start_process:
                            # remove empty rows
                            if not row[0]:
                                continue

                            raw_item = {head.upper(): row[idx] for idx, head in enumerate(header)}
                            raw_item.update(
                                provider_name=self.provider,
                                reported_date=self.reported_date,
                                port_name=sheet.name,
                                year=year,
                            )
                            if DataTypes.SpotCharter in self.produces:
                                yield normalize_charters.process_item(raw_item)
                            else:
                                yield from normalize_grades.process_item(raw_item)


class SeoulLineFixturesSpider(SeoulLineSpider):
    name = 'SEL_Fixtures'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
        # prevent spider from marking email as seen
        # multiple spiders to run
        'MARK_MAIL_AS_SEEN': False,
    }


class SeoulLineGradesSpider(SeoulLineSpider):
    name = 'SEL_Grades'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
