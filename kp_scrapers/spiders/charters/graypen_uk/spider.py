from dateutil.parser import parse as parse_date
import xlrd

from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.graypen_uk import normalize_charters, normalize_grades


ATTACHMENT_PLATFORM_MAPPING = {
    'oil': ['crude', 'fuel'],
    'lpg': ['lpg'],
    'cpp': ['cpp', 'gasoline', 'diesel', 'naphtha'],
}


class GraypenUKSpider(CharterSpider, MailSpider):
    name = 'GP_UK'
    provider = 'Graypen'
    version = '1.1.0'

    def parse_mail(self, mail):
        """The method will be called for every mail the search_term matched.
        """
        # FIXME charters loader assumes `dayfirst=True` when parsing, so we can't use ISO-8601
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')
        # kp_excel = KplerApiService(self.platform_name)

        for attachment in mail.attachments():
            for plt, attachment_name in ATTACHMENT_PLATFORM_MAPPING.items():
                if any(sub in attachment.name.replace('.', '').lower() for sub in attachment_name):
                    platform = plt
                else:
                    platform = None

                # filter out unnecessary attachments
                if not platform:
                    continue

            # extract relevant data from each sheet within the xlsx
            for sheet in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets():

                for raw_item in self.parse_sheet(sheet):
                    # contextualise raw item with meta information
                    raw_item.update(
                        movement=self._extract_movement(sheet),
                        platform=platform,
                        provider_name=self.provider,
                        reported_date=self.reported_date,
                    )
                    if DataTypes.SpotCharter in self.produces:
                        yield normalize_charters.process_item(raw_item)
                    # FIXME supposed to be `DataTypes.PortCall` here, but we don't want
                    # data-dispatcher to consume data from these spiders and the ETL
                    # to create PCs
                    elif DataTypes.Cargo in self.produces:
                        yield normalize_grades.process_item(raw_item)

    def parse_sheet(self, sheet):
        """Extract raw table data from specified sheet.

        Args:
            sheet (xlrd.Sheet):

        Yields:
            Dict[str, str]:

        """
        header = None
        for idx, row in enumerate(sheet.get_rows()):
            # first row will always be the header
            if idx == 0:
                header = [cell.value for cell in row]
                continue

            row = [
                xldate_to_datetime(cell.value, sheet.book.datemode).isoformat()
                if is_xldate(cell)
                else cell.value
                for cell in row
            ]
            yield {head: row[head_idx] for head_idx, head in enumerate(header)}

    @staticmethod
    def _extract_movement(sheet):
        return 'load' if 'export' in sheet.name.lower() else 'discharge'


class GPUKFixturesSpider(GraypenUKSpider):
    name = 'GP_UKFixtures'
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


class GPUKGradesSpider(GraypenUKSpider):
    name = 'GP_UKGrades'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
