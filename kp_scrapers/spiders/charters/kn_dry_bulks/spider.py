from dateutil.parser import parse as parse_date
import xlrd

from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.kn_dry_bulks import normalize_charters, normalize_grades


class KNDryBulks(CharterSpider, MailSpider):

    provider = 'Kanoo'
    version = '1.0.0'

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
            for sheet in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets():

                prev_row = None

                for idx, row in enumerate(sheet.get_rows()):

                    row = [
                        xldate_to_datetime(cell.value, sheet.book.datemode).isoformat()
                        if is_xldate(cell)
                        else str(cell.value)
                        for cell in row
                    ]

                    # first row is useless, discard it
                    if idx == 0:
                        continue

                    # second row will always contain header; extract it
                    if 'PORT' in row[0]:
                        header = [may_strip(cell) for cell in row]
                        continue

                    # remove empty rows
                    if not row[0]:
                        continue

                    # some rows are merged, store first item and 'forward fill'
                    if row[1] != '':
                        prev_row = row

                    elif row[1] == '' and len(row) == len(prev_row) and prev_row:
                        for i, x in enumerate(row):
                            if row[i] == '':
                                row[i] = prev_row[i]

                    # extract data row
                    raw_item = {head: row[head_idx] for head_idx, head in enumerate(header)}

                    raw_item.update(reported_date=reported_date, provider_name=self.provider)

                    if DataTypes.SpotCharter in self.produces:
                        yield normalize_charters.process_item(raw_item)
                    # FIXME supposed to be `DataTypes.PortCall` here, but we don't want
                    # data-dispatcher to consume data from these spiders
                    # and the ETL to create PCs
                    else:
                        yield normalize_grades.process_item(raw_item)


class KNDryBulksCharters(KNDryBulks):
    name = 'KN_DryBulk_Charters'
    produces = [DataTypes.SpotCharter]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
        # prevent spider from marking the email as seen
        # so that multiple spiders can run on it
        'MARK_MAIL_AS_SEEN': False,
    }


class KNDryBulksGrades(KNDryBulks):
    name = 'KN_DryBulk_Grades'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
