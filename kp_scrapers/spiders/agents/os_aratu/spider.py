import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.os_aratu.normalize import process_item
from kp_scrapers.spiders.agents.os_aratu.parser import TABULA_OPTIONS
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider


MISSING_ROWS = []


class SShipAratuSpider(ShipAgentMixin, PdfSpider, MailSpider):
    name = 'SShip_Aratu'
    provider = 'Starship'
    version = '0.1.1'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def __init__(self, columns=None, area=None, *args, **kwargs):
        """
        PDF might differ, input custom area and column dimension to overwrite
        Args:
            columns (str): coords in px of columns, 11 values
            area (str): area of interest in px, 4 values - y1,x1,y2,x2
                y1 = top
                x1 = left
                y2 = top + height
                x2 = left + width
        """
        super().__init__(*args, **kwargs)
        self.indicator = columns
        self.tab_option = {
            '--stream': [],
            '--pages': ['all'],
            '--columns': [columns],
            '--area': [area],
        }

    def parse_mail(self, mail):
        """The method will be called for every mail the search_term matched.
        """
        for attachment in mail.attachments():
            if not attachment.is_pdf:
                continue

            if self.indicator:
                self.logger.info('overwriting tabula settings')

                yield from self.parse_pdf(attachment.name, attachment.body, self.tab_option, mail)
            else:
                for opt in TABULA_OPTIONS:
                    try:
                        yield from self.parse_pdf(attachment.name, attachment.body, opt, mail)
                        return
                    except Exception:
                        continue

                self.logger.error('unable to parse {}'.format(mail.envelope['subject']))
                MISSING_ROWS.append(may_strip(mail.envelope['subject']))

    def parse_pdf(self, p_name, body, tab_opt, mail):
        """Parse PDF reports.
        Args:
            attachment (Attachment): mail attachment object
        Yields
            Dict[str, str]:
        """
        prev_row = None
        reported_date = self.extract_reported_date(mail.envelope['subject'])

        for idx, row in enumerate(self.extract_pdf_io(body, **tab_opt)):
            # remove rows with no vessels
            if not row[1]:
                continue

            # memoise row for ffill
            prev_row = row if row[1] else prev_row

            # remove filler rows
            if any(sub in row[1] for sub in ('EMPTY', 'TGL', 'TERMINAL', 'TPG')):
                continue

            if 'VESSEL' in row[1]:
                header = row
                continue

            if not row[1]:
                row = [
                    prev_r if not row[prev_idx] else row[prev_idx]
                    for prev_idx, prev_r in enumerate(prev_row)
                ]

            # extract data row
            raw_item = {head: may_strip(row[head_idx]) for head_idx, head in enumerate(header)}
            # contextualise raw item with meta info
            raw_item.update(
                reported_date=reported_date,
                port_name='Aratu',  # report only contains data for Aratu port
                provider_name=self.provider,
            )

            yield process_item(raw_item)

    @staticmethod
    def extract_reported_date(mail_title):
        _match = re.match(r'.*?(\d{2}\/\d{2}\/\d{4}).*', mail_title)
        if _match:
            r_date = _match.group(1)

            return to_isoformat(r_date, dayfirst=True)

    @property
    def missing_rows(self):
        return MISSING_ROWS
