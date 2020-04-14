from scrapy import Request
import xlrd

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import format_cells_in_row
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.benline_grades import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


IRON_PORTS = {
    'dampier': ('Dampier', 'Dampier'),
    'port walcott': ('Port Walcott', None),
    'port hedland': ('Port Hedland', None),
    'port hedland fmg': ('Port Hedland', None),
    'port latta': ('Latta', 'Latta'),
    'geraldton': ('Geraldton', 'Geraldton'),
    'geraldton karara': ('Geraldton', 'Geraldton'),
    'esperance': ('Esperance', 'Esperance'),
}


COAL_PORTS = {
    'abbot point': ('Abbot Point', 'Abbot Point'),
    'brisbane': ('Brisbane', None),
    'hay point': ('Hay Reef', 'Hay Point'),
    'dalrymple bay coal berth': ('Hay Reef', 'Dalrymple Bay'),
    'dalryple bay': ('Hay Reef', 'Dalrymple Bay'),
    'gladstone': ('Gladstone', None),
    'pwcs - newcastle': ('Newcastle', 'PWCS'),
    'pwcs - carrington, newcastle': ('Newcastle', 'PWCS Carrington Terminal'),
    'pwcs - kooragang, newcastle': ('Newcastle', 'PWCS Kooragang Terminal'),
    'pwcs - unallocated terminal': ('Newcastle', None),
    'ncig': ('Newcastle', 'NCIG Kooragang'),
    'ncig, newcastle': ('Newcastle', 'NCIG Kooragang'),
    'port kembla': ('Kembla', 'Port Kembla'),
}


class BenLineGradesSpider(ShipAgentMixin, MailSpider):
    name = 'BL_Dry_Grades'
    provider = 'Ben Line'
    version = '1.1.1'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """parse 2 email attachments iron and coal for australia

        Args:
            mail (Mail):
        Yields:
            Dict[str, str]:
        """
        # memoise reported date so it does not need to be called repeatedly later
        reported_date = to_isoformat(mail.envelope['date'])

        if len(list(mail.attachments())) == 0:
            body = self.select_body_html(mail)
            excel_link = body.xpath('//a/@href').extract()
            if any(term in mail.envelope['subject'].lower() for term in ('regional', 'group')):
                yield Request(
                    url=excel_link[1],
                    callback=self.parse_xls_report,
                    meta={'reported_date': reported_date},
                )
            else:
                yield Request(
                    url=excel_link[1],
                    callback=self.parse_weekly,
                    meta={'reported_date': reported_date},
                )

        if len(list(mail.attachments())) > 0:
            for attachment in mail.attachments():
                # sanity check in case file is not a spreadsheet
                if not attachment.is_spreadsheet:
                    continue

                # to handle different excel formats
                if 'agencies' in attachment.name.lower():
                    yield from self.parse_xls_report(
                        response=None, e_body=attachment, email_rpt_date=reported_date
                    )
                else:
                    yield from self.parse_weekly(
                        response=None, e_body=attachment, email_rpt_date=reported_date
                    )

    # processes the weekly reports from ben line
    def parse_weekly(self, response, e_body=None, email_rpt_date=None):
        # get appropriate response from link or web
        if response:
            info_body = response
            info_body_name = str(info_body)
            info_rpt_date = info_body.meta.get('reported_date')
        else:
            info_body = e_body
            info_body_name = info_body.name
            info_rpt_date = email_rpt_date

        # port names are fixed and appended above each section
        if 'iron' in info_body_name.lower():
            PORTS = IRON_PORTS
            COMMODITY = 'iron'

        if 'coal' in info_body_name.lower():
            PORTS = COAL_PORTS
            COMMODITY = 'coal'

        for sheet in xlrd.open_workbook(file_contents=info_body.body, on_demand=True).sheets():
            if sheet.name.isdigit() or sheet.name.lower() in ('vessel line up'):
                header = None
                port_name = None
                installation = None
                for idx, raw_row in enumerate(sheet.get_rows()):
                    row = format_cells_in_row(raw_row, sheet.book.datemode)

                    # detect if cell is a port cell and memoise it
                    if row[0].lower() in PORTS:
                        port_name = PORTS[row[0].lower()][0]
                        installation = PORTS[row[0].lower()][1]

                    # detect header row
                    if row[0].lower() == 'vessel':
                        header = row
                        continue

                    if header:
                        raw_item = {h.lower(): row[idx] for idx, h in enumerate(header)}
                        raw_item.update(
                            provider_name=self.provider,
                            reported_date=info_rpt_date,
                            port_name=port_name,
                            installation=installation,
                            file_name=COMMODITY,
                        )
                        yield normalize.process_item(raw_item)

    # ben line one off reports i.e FW: Australia Line Up 2018 & 2019
    def parse_xls_report(self, response, e_body=None, email_rpt_date=None):
        # get appropriate response from link or web
        if response:
            info_body = response
            info_rpt_date = info_body.meta.get('reported_date')
        else:
            info_body = e_body
            info_rpt_date = email_rpt_date
        for sheet in xlrd.open_workbook(file_contents=info_body.body, on_demand=True).sheets():
            processing = False
            for idx, raw_row in enumerate(sheet.get_rows()):
                row = format_cells_in_row(raw_row, sheet.book.datemode)
                # detect header row and rows to process
                if 'country' in row[0].lower():
                    header = row
                    processing = True
                    continue

                if processing:
                    raw_item = {h.lower(): row[idx] for idx, h in enumerate(header)}
                    raw_item.update(provider_name=self.provider, reported_date=info_rpt_date)
                    yield normalize.process_item(raw_item)
