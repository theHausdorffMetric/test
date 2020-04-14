import xlrd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.pi_venezuela import normalize_charters, normalize_grades


class PIVenezuelaSpider(CharterSpider, MailSpider):

    name = 'PI_Venezuela'
    provider = 'PI'
    version = '1.0.0'

    def parse_mail(self, mail):
        """The method will be called for every mail the search_term matched.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:

        """
        for attachment in mail.attachments():
            for sheet in xlrd.open_workbook(file_contents=attachment.body, on_demand=True).sheets():
                # only process the sheet we want
                if not any(sub in sheet.name.lower() for sub in ('export', 'import')):
                    continue

                sheet_name = sheet.name.lower()

                for idx, raw_row in enumerate(sheet.get_rows()):
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

                    if idx == 0:
                        reported_date = to_isoformat(row[1], dayfirst=True)
                        continue

                    # second row will always contain header; extract it
                    if idx == 1:
                        header = row
                        continue

                    # extract data row
                    raw_item = {head: row[head_idx] for head_idx, head in enumerate(header)}
                    raw_item.update(
                        reported_date=reported_date,
                        provider_name=self.provider,
                        sheet_name=sheet_name,
                    )

                    if DataTypes.SpotCharter in self.produces:
                        print(raw_item)
                        yield normalize_charters.process_item(raw_item)
                    else:
                        yield normalize_grades.process_item(raw_item)


class PIVenezuelaChartersSpider(PIVenezuelaSpider):
    name = 'PI_Venezuela_Charters'
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


class PIVenezuelaGradesSpider(PIVenezuelaSpider):
    name = 'PI_Venezuela_Grades'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
