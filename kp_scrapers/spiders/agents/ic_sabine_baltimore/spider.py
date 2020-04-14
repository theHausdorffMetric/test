from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.ic_sabine_baltimore import normalize
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider


class ICSabineBaltimoreSpider(ShipAgentMixin, PdfSpider, MailSpider):
    name = 'IC_Sabine_Baltimore'
    provider = 'ISS'
    version = '0.1.1'
    # we want to create portcalls from this commercial report, hence PortCall model is used
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    tabula_options = {'--pages': ['all'], '--lattice': [], '--area': ['116.03,11.18,825.76,585.46']}

    def parse_mail(self, mail):
        """The method will be called for every mail the search_term matched.
        """
        reported_date = to_isoformat(mail.envelope['date'])

        for attachment in mail.attachments():
            if not attachment.is_pdf:
                continue

            yield from self.parse_pdf(attachment.body, reported_date, mail.envelope['subject'])

    def parse_pdf(self, body, rpt_date, subject_name):
        table = self.extract_pdf_io(body, **self.tabula_options)

        for idx, raw_row in enumerate(table):
            row = [cell for cell in raw_row if may_strip(cell)]

            # get the installation name
            if len(row) == 1:
                installation = row[0]
                continue

            if 'vessel' in row[0].lower():
                header = row
                continue

            raw_item = {header[idx]: cell for idx, cell in enumerate(row)}
            raw_item.update(
                reported_date=rpt_date,
                provider_name=self.provider,
                installation=installation,
                attachment_name=subject_name,
            )
            yield normalize.process_item(raw_item)
