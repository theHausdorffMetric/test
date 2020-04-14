from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.pf_gdansk import normalize_charters, normalize_grades


MISSING_ROWS = []


class GdanskSpider(MailSpider):
    def parse_mail(self, mail):
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')
        body = self.select_body_html(mail)
        cargo_movement = None
        vessel_processing = False
        line_list = []
        # this is used as a flag to denote whether the program has started reading a single record
        for paragraph in body.xpath('//p'):
            row = paragraph.xpath('.//text()').extract()

            # import and export are two different section in the email. Most emails only has
            # import section. Also detect when to start processing
            line = may_strip(' '.join(row).lower())
            if 'import' in row[0]:
                cargo_movement = 'discharge'
                continue
            if 'export' in row[0]:
                cargo_movement = 'load'
                continue

            if 'm/t' in line:
                vessel_processing = True
                line_list = []

            if 'cargo receiver' in line:
                vessel_processing = False
                raw_item = {
                    'raw_string': '|'.join(line_list),
                    'provider_name': self.provider,
                    'reported_date': self.reported_date,
                    'port_name': self.port_name,
                    'cargo_movement': cargo_movement,
                }
                if DataTypes.SpotCharter in self.produces:
                    yield normalize_charters.process_item(raw_item)
                else:
                    yield normalize_grades.process_item(raw_item)

            if vessel_processing:
                line_list.append(line)

    @property
    def missing_rows(self):
        return MISSING_ROWS


class GdanskCharterSpider(CharterSpider, GdanskSpider):
    name = 'PF_Gdansk_Charters'
    provider = 'Polfract'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel, DataTypes.Cargo]
    version = '2.0.1'
    port_name = 'Gdansk'

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
        'MARK_MAIL_AS_SEEN': False,
    }


class GdanskPortcallSpider(ShipAgentMixin, GdanskSpider):
    name = 'PF_Gdansk_Grades'
    provider = 'Polfract'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]
    version = '2.0.1'
    port_name = 'Gdansk'
    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
