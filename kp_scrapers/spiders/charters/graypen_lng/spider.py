from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.graypen_lng import normalize_charters, normalize_portcalls
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


class GraypenLNGSpider(PdfSpider, MailSpider):
    provider = 'Graypen'
    version = '1.0.0'

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

            yield from self.parse_pdf_table(attachment)

    def parse_pdf_table(self, attachment):
        """Extract raw data from PDF attachment in email.

        Args:
            attachment (Attachment):

        Yields:
            Dict[str, str]:
        """
        for idx, row in enumerate(self.extract_pdf_io(attachment.body, **self.tabula_options)):
            # remove unnecessary rows
            if len(row) < 12:
                continue

            if not row[2]:
                continue

            # extract headers (do not extract them since the way they are extracted is inconsistent)
            if any('vessel' in cell.lower() for cell in row):
                header = row
                continue

            # there are 12 cols, however, part of the table could be truncated to the next page
            # with empty date cols. this would result in tabula parsing only 10 cols
            if len(row) == 10:
                while len(row) != 12:
                    row.insert(3, '')

            raw_item = {head: row[head_idx] for head_idx, head in enumerate(header)}
            # contextualise raw item with some meta info
            raw_item.update(provider_name=self.provider, reported_date=self.reported_date)

            if DataTypes.SpotCharter in self.produces:
                yield normalize_charters.process_item(raw_item)
            else:
                yield normalize_portcalls.process_item(raw_item)


class GraypenLNGFixtureSpider(GraypenLNGSpider, CharterSpider):
    name = 'GP_LNG_Fixtures'
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


class GraypenLNGPortCallSpider(GraypenLNGSpider, PortAuthoritySpider):
    name = 'GP_LNG_PortCalls'
    produces = [DataTypes.PortCall]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
