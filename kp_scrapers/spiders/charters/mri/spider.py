import re

import xlrd

from kp_scrapers.lib.excel import is_xldate, xldate_to_datetime
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.mri import normalize_charters, normalize_grades


HEADER_ROW_IDX = 0
COMMODITY_COLUMN_INDEX = 4
VESSEL_COLUMN_INDEX = 3


class MriBaseSpider(CharterSpider, MailSpider):
    provider = 'MRI'
    version = '1.0.0'

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': False,
        # allow multiple spiders to run on the same email in sequence
        'MARK_MAIL_AS_SEEN': False,
    }

    # abstract property for derived spiders to use and filter rows
    commodity_type = None

    def parse_mail(self, mail):
        """Extract mail found with specified email filters in spider arguments.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:
        """
        for attachment in mail.attachments():
            # memoise reported date so it won't need to be called repeatedly later
            _match = re.match(r'.*DBE\s(\d+)', attachment.name)
            if not _match:
                raise ValueError(f'Unknown reported date format: {attachment.name}')
            reported_date = _match.group(1)

            # each xlsx file by this provider will only have one sheet
            sheet = xlrd.open_workbook(
                file_contents=attachment.body, on_demand=True
            ).sheet_by_index(0)

            # extract raw data from sheet
            for idx, row in enumerate(sheet.get_rows()):
                row = [
                    xldate_to_datetime(cell.value, sheet.book.datemode).isoformat()
                    if is_xldate(cell)
                    else str(cell.value)
                    for cell in row
                ]

                # initialise headers
                if idx == HEADER_ROW_IDX:
                    header = row
                    continue

                # skip vessels with () inside, i.e., vessel has not been identified by provider
                if '(' in row[VESSEL_COLUMN_INDEX]:
                    continue

                # FIXME timecharter type
                if 'timecharter' in row[COMMODITY_COLUMN_INDEX].lower():
                    continue

                raw_item = {head: row[idx] for idx, head in enumerate(header)}
                # contextualise raw item with metadata
                raw_item.update(
                    provider_name='MRI', reported_date=reported_date, spider_name=self.name
                )

                if DataTypes.SpotCharter in self.produces:
                    yield normalize_charters.process_item(raw_item)
                # FIXME supposed to be `DataTypes.PortCall` here, but we don't want
                # data-dispatcher to consume data from these spiders and the ETL to create PCs
                else:
                    yield normalize_grades.process_item(raw_item)


class MriFixturesSpider(MriBaseSpider):
    """Spider to process attachment for CPP platform only.
    """

    name = 'MRI_Fixtures'
    produces = [DataTypes.SpotCharter]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }


class MriGradesSpider(MriBaseSpider):
    """Spider to process attachment for CPP platform only.
    """

    name = 'MRI_Grades'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
