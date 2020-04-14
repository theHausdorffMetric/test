import xlrd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.butinge_klaipeda import normalize_charters, normalize_grades


class KlaipedaButingeSpider(CharterSpider, MailSpider):
    name = 'UN_KlaipedaButinge'
    provider = 'Unitek'
    version = '1.1.0'

    def parse_mail(self, mail):
        """Extract mail found with specified email filters in spider arguments.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:
        """
        self.reported_date = to_isoformat(mail.envelope['date'])

        for attachment in mail.attachments():
            for file_in_mail in self.relevant_files:
                sheet = xlrd.open_workbook(
                    file_contents=attachment.body, on_demand=True
                ).sheet_by_name(file_in_mail)

                for raw_row in sheet.get_rows():
                    row = format_cells_in_row(raw_row, sheet.book.datemode)

                    # ignore irrelevant rows
                    if not row[0]:
                        continue

                    # extract header row, rpeort headers are inconsistent
                    if 'PORT' in row[0].upper():
                        header = row
                        continue

                    raw_item = {head.upper(): row[idx] for idx, head in enumerate(header)}
                    raw_item.update(
                        provider_name=self.provider,
                        reported_date=self.reported_date,
                        sheet_name=sheet.name,
                        spider_name=self.name,
                    )

                    if DataTypes.SpotCharter in self.produces:
                        yield normalize_charters.process_item(raw_item)
                    else:
                        yield normalize_grades.process_item(raw_item)


class KlaipedaButingeFixturesSpider(KlaipedaButingeSpider):
    name = 'UN_KlaipedaButinge_Fixtures'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]
    relevant_files = ['Klaipeda', 'Butinge SPM']

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
        # prevent spider from marking email as seen
        # multiple spiders to run
        'MARK_MAIL_AS_SEEN': False,
    }


class KlaipedaButingeGradesSpider(KlaipedaButingeSpider):
    name = 'UN_KlaipedaButinge_Grades'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]
    relevant_files = ['Klaipeda', 'Butinge SPM']

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
