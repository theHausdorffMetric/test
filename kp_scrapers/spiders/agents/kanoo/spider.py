import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.kanoo import normalize
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider


class KanooSpider(ShipAgentMixin, PdfSpider, MailSpider):
    name = 'KN_SouthAfrica_Grades'
    provider = 'Kanoo'
    version = '1.0.0'

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    tabula_options = {'--guess': [], '--pages': ['all'], '--stream': [], '--lattice': []}

    def parse_mail(self, mail):
        """Entry point of KN_SouthAfrica_Grades spider.

        Args:
            mail (Mail):

        Yields:
            Portcall:

        """
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        for attachment in mail.attachments():
            # A single email contains a lot of attachments.
            # Here, we filter out few files based on the names, as those files
            # are not needed for processing.
            if re.search(
                r'PE Bunker(.)*|Port Elizabeth(.)*|(.)*ETA list(.*)|(.)*Coal(.*)', attachment.name
            ):
                continue

            if attachment.is_pdf:
                yield from self.parse_pdf(attachment)

    def parse_pdf(self, attachment):
        """Parse pdf table.

        Args:
            body (Body): attachment
        Yields:
            Dict[str, Any]
        """
        table = self.extract_pdf_io(attachment.body, **self.tabula_options)
        PROCESSING_STARTED = False
        # get the port_name from the filename
        port_name, *_ = attachment.name.partition('-')
        for row in table:
            # row containing VESSEL is the header row in the source file.
            if 'VESSEL' in row:
                PROCESSING_STARTED = True
                header_row = [may_strip(val) for val in row if val]
                continue

            if PROCESSING_STARTED:
                if _row_filtering_condition(row):
                    value = [may_strip(val) for val in row]
                    raw_item = dict(zip(header_row, value))
                    raw_item.update(
                        {
                            'reported_date': self.reported_date,
                            'provider_name': self.provider,
                            'port_name': port_name,
                        }
                    )
                    yield normalize.process_item(raw_item)
                else:
                    PROCESSING_STARTED = False

        return


def _row_filtering_condition(row):
    """Filter condition to get the original row

    Args:
        List[str]
    Return:
        Bool
    """

    if (
        (row[0])
        and (may_strip(row[0]) not in ['REMARKS:'])
        and (len([may_strip(val) for val in row if val]) > 2)  # to avoid header/invalid rows
    ):
        return True
