from io import BytesIO
import re
from zipfile import ZipFile

from dateutil.parser import parse as parse_date
import xlrd

from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.kanoo_regional import normalize_charters, normalize_grades
from kp_scrapers.spiders.bases.mail import MailSpider


HEADER_PATTERN = 'Charterer Name'
EMPTY_ROW_INDICATOR_INDEX = 4
# crude oil and liquid attachments are ignored for now because the volumes matter
ACCEPTED_DOCS = ['chemical', 'chemicals', 'products', 'product']


class KanooRegionalSpider(ShipAgentMixin, MailSpider):
    name = 'KN_Regional'
    provider = 'Kanoo'
    version = '1.0.0'

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': False,
    }

    def parse_mail(self, mail):
        """The method will be called for every mail the search_term matched.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:

        """
        # memoise and use received timestamp of email as default reported date
        reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        for attachment in mail.attachments():

            # take reported date in sheet name if it contains one, else use default
            reported_date = self.parse_reported_date(attachment.name) or reported_date

            # attachment can be in excel or zipped together with other files
            if attachment.is_spreadsheet:
                doc = attachment.body
                yield from self.parse_attachment(doc, reported_date)

            elif attachment.is_zip:
                # TODO: create a library to extract zip files
                z = ZipFile(BytesIO(attachment.body))

                for document in z.namelist():
                    for x in ACCEPTED_DOCS:
                        if x in document.lower():
                            doc = z.read(document)
                            yield from self.parse_attachment(doc, reported_date)

    def parse_attachment(self, attachment_doc, reported_date):
        # each xlsx file by this provider will only have one sheet
        sheet = xlrd.open_workbook(file_contents=attachment_doc, on_demand=True).sheet_by_name(
            'Market Data Report'
        )
        # for this spot charter we don't look at cargo information (already saved
        # in corresponding cargo movement)
        # when several rows for a vessel with same charter values,port values and
        # arrival values we only build one item
        spot_charters_seen = {}
        for idx, row in enumerate(sheet.get_rows()):
            row = [
                xldate_to_datetime(cell.value, sheet.book.datemode).isoformat()
                if is_xldate(cell)
                else str(cell.value)
                for cell in row
            ]

            # remove empty rows before and after the main data table
            # denoted by a falsy value
            if not row[EMPTY_ROW_INDICATOR_INDEX]:
                continue

            # initialise headers
            if HEADER_PATTERN in row:
                header = row
                continue

            # extract data row
            raw_item = {head: row[head_idx] for head_idx, head in enumerate(header)}
            raw_item.update(reported_date=reported_date, provider_name=self.provider)
            if DataTypes.SpotCharter in self.produces:
                if (
                    raw_item['Charterer Name']
                    + raw_item['Vessel']
                    + raw_item['Arrival'].split(' ')[0]
                    + raw_item['Port Name']
                    in spot_charters_seen.keys()
                ):
                    continue
                spot_charters_seen[
                    raw_item['Charterer Name']
                    + raw_item['Vessel']
                    + raw_item['Arrival'].split(' ')[0]
                    + raw_item['Port Name']
                ] = True
                yield normalize_charters.process_item(raw_item)
            elif DataTypes.Cargo in self.produces:
                yield normalize_grades.process_item(raw_item)

    @staticmethod
    def parse_reported_date(raw_reported_date):
        """Normalize raw reported date to a valid format string.

        Args:
            raw_date (str)

        Returns:
            str | None:

        """
        date_match = re.match(r'.*?(\d{1,2}\.\d{1,2}\.\d{1,4}).*', raw_reported_date)
        if date_match:
            return parse_date(date_match.group(1), dayfirst=True).strftime('%d %b %Y')


class KanooRegionalCharterSpider(KanooRegionalSpider):
    name = 'KN_Regional_Charters'
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


class KanooRegionalGradesSpider(KanooRegionalSpider):
    name = 'KN_Regional_Grades'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
        # prevent spider from marking the email as seen
        # so that multiple spiders can run on it
    }
