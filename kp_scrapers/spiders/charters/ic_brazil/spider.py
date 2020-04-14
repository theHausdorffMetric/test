import xlrd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.ic_brazil import normalize_charters, normalize_grades


START_PROCESSING_WORD = ['VESSEL', 'TYPE OF LINE UP']
STOP_PROCESSING_WORD = ''
BLACKLIST_SHEETS = ['summary', 'tables']


class ICBrazilSpider(MailSpider):

    provider = 'ISS'
    version = '1.0.0'

    def parse_mail(self, mail):
        """Parse each mail matched by the `query` spider argument.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:

        """
        reported_date = to_isoformat(mail.envelope['date'])

        for attachment in mail.attachments():
            # sanity check
            if not attachment.is_pdf:
                doc = attachment.body
                yield from self.parse_attachment(doc, reported_date)

    def parse_attachment(self, attachment_doc, reported_date):

        workbook = xlrd.open_workbook(file_contents=attachment_doc, on_demand=True)

        for sheet in workbook.sheets():

            if sheet.name.lower() not in BLACKLIST_SHEETS:
                # assign variable to detect which row to start processing
                start_processing = False
                raw_port_name = None

                # store state of the table, in order to get relevant rows to extract
                for raw_row in sheet.get_rows():
                    # Include handling of xlrd.xldate.XLDateAmbiguous cases
                    row = []
                    for cell in raw_row:
                        if is_xldate(cell):
                            try:
                                cell = xldate_to_datetime(
                                    cell.value, sheet.book.datemode
                                ).isoformat()
                            except Exception:
                                cell = str(cell.value)

                        else:
                            cell = str(cell.value)

                        row.append(cell)

                    # detect portname as some tabs/attachments have the port name above
                    # the data row
                    if 'Port' in row[0]:
                        raw_port_name = row[0]
                        continue

                    # detect relevant row
                    if row[0] in START_PROCESSING_WORD:
                        start_processing = True
                        header = row
                        continue

                    # detect irrelevant row
                    if row[0] == STOP_PROCESSING_WORD:
                        start_processing = False
                        continue

                    if start_processing:
                        # remove unnecessary rows
                        if row[0] == '' or 'Without' in row[0] or ':' in row[0]:
                            continue

                        raw_item = {head: row[idx] for idx, head in enumerate(header)}
                        # contextualise raw item with meta info
                        raw_item.update(
                            reported_date=reported_date,
                            provider_name=self.provider,
                            raw_port_name=raw_port_name,
                        )

                        if DataTypes.SpotCharter in self.produces:
                            yield normalize_charters.process_item(raw_item)

                        if DataTypes.Cargo in self.produces:
                            yield from normalize_grades.process_item(raw_item)


class ICBrazilFixturesLiquidsSpider(ICBrazilSpider, CharterSpider):
    """Spider to process attachment for spot charters
    """

    name = 'IC_BrazilLiquids_Fixtures'
    produces = [DataTypes.SpotCharter]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
        'MARK_MAIL_AS_SEEN': False,
    }


class ICBrazilGradesFixturesSpider(ICBrazilSpider, ShipAgentMixin):
    """Spider to process attachment grades
    """

    name = 'IC_Brazil_Grades'
    produces = [DataTypes.Cargo, DataTypes.Vessel, DataTypes.CargoMovement]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
