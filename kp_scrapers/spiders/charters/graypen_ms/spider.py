from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.graypen_ms import normalize_charters, normalize_grades, parser


MISSING_ROWS = []


class GraypenMongstadStureSpider(CharterSpider, PdfSpider, MailSpider):
    name = 'GP_MongstadSture'
    provider = 'Graypen'
    version = '1.1.1'

    tabula_options = {'--guess': [], '--pages': ['all'], '--stream': []}

    def parse_mail(self, mail):
        """Extract mail found with specified email filters in spider arguments.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:
        """
        self.reported_date = to_isoformat(mail.envelope['date'])

        for attachment in mail.attachments():
            # sanity check, in case we receive irrelevant files
            if not attachment.is_pdf:
                continue

            # only process reports with these naming patterns
            for pattern in ['mongstad', 'sture']:
                if pattern in attachment.name.lower():
                    yield from self.parse_pdf_table(attachment)

    def parse_pdf_table(self, attachment):
        """Extract raw data from PDF attachment in email.

        Args:
            attachment (Attachment):

        Yields:
            Dict[str, str]:
        """
        for idx, row in enumerate(self.extract_pdf_io(attachment.body, **self.tabula_options)):
            # discard useless, irrelevant row
            # some rows will not provide `Next Port` or `Notes` column data
            if row.count('') > 2:
                continue

            # extract headers (do not extract them since the way they are extracted is inconsistent)
            if any('vessel' in cell.lower() for cell in row):
                header = parser.parse_header(row)
                continue

            row = parser.parse_data_row(row)
            if len(row) == len(header):
                raw_item = {head: row[head_idx] for head_idx, head in enumerate(header)}
                # contextualise raw item with some meta info
                raw_item.update(
                    # port name in reports are only a single word
                    port_name=attachment.name.split()[0],
                    provider_name=self.provider,
                    reported_date=self.reported_date,
                )
                if DataTypes.SpotCharter in self.produces:
                    yield normalize_charters.process_item(raw_item)
                # FIXME supposed to be `DataTypes.PortCall` here, but we don't want
                # data-dispatcher to consume data from these spiders and the ETL to create PCs
                else:
                    yield normalize_grades.process_item(raw_item)
            else:
                MISSING_ROWS.append(str(row))

    @property
    def missing_rows(self):
        return MISSING_ROWS


class GraypenMongstadStureFixturesSpider(GraypenMongstadStureSpider):
    name = 'GP_MongstadSture_Fixtures'
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


class GraypenMongstadStureGradesSpider(GraypenMongstadStureSpider):
    name = 'GP_MongstadSture_Grades'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
