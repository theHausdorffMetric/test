from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.os_sarroch import normalize
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider


CARGO_COL_IDX = 9
ETA_COL_IDX = 3


class SarrochSpider(ShipAgentMixin, PdfSpider, MailSpider):
    name = 'OS_Sarroch_Grades'
    provider = 'Norlarma'
    version = '1.1.2'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    tabula_options = {'--guess': [], '--lattice': []}

    # source only provides data for this specific port, and nowhere else
    port_name = 'Sarroch'

    def parse_mail(self, mail):
        # memoise reported date so it won't need to be computed repeatedly later on
        reported_date = to_isoformat(mail.envelope['date'])

        for attachment in mail.attachments():
            # sanity check, in case non-pdf files are sent
            if not attachment.is_pdf:
                continue

            table = self.extract_pdf_io(attachment.body, **self.tabula_options)
            for row in table:
                # ensure row if of appropriate length
                if len(row) < 4:
                    continue
                # insert empty cell to row if the dates are combined into a single cell,
                # to properly sanitize it for the later steps
                if 'TBC' in row[ETA_COL_IDX] and not row[-1]:
                    for back_elem in row[::-1]:
                        # stop mutating `row` once we fill up the middle columns
                        if back_elem:
                            break

                        row.insert(ETA_COL_IDX, '')

                # yield only if row is properly parsed and contains cargo
                if len(row) > CARGO_COL_IDX and row[CARGO_COL_IDX]:
                    raw_item = {str(idx): cell for idx, cell in enumerate(row)}
                    raw_item.update(
                        port_name=self.port_name,
                        provider_name=self.provider,
                        reported_date=reported_date,
                    )
                    yield from normalize.process_item(raw_item)
