from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.gibson_algeria import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


ROW_LEN_LIMIT = 10
HEADER_SIGN = 'VESSEL'

PORT_NAME = ['Arzew', 'ALGIERS', 'BEJAIA', 'SKIKDA']


class GibsonAlgeriaSpider(ShipAgentMixin, MailSpider):
    name = 'GB_EA_Algeria_Grades'
    provider = 'Sermarine'
    version = '1.0.1'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    reported_date = None
    missing_rows = []

    def parse_mail(self, mail):
        """Entry point of GB_EA_Algeria_Grades spider to get cargo movement info.

        Args:
            mail:

        Returns:
            PortCall:

        """
        self.reported_date = to_isoformat(mail.envelope['date'])
        header = None
        table_count = 0
        for row_sel in self.select_body_html(mail).xpath('//tr'):
            raw_row = [
                may_strip(''.join(td.xpath('.//text()').extract())) for td in row_sel.xpath('.//td')
            ]
            # this is based on the hypothesis that all the fields will not be empty for a valid
            # row, so the header and row would still align
            row = [cell for cell in raw_row if cell]

            if len(row) > ROW_LEN_LIMIT:
                continue

            if HEADER_SIGN in row:
                header = row
                table_count += 1
                continue

            if header and len(header) == len(row):
                raw_item = {header[idx]: cell for idx, cell in enumerate(row)}
                raw_item.update(self.meta_field)
                # we have four port, each of them have four tables
                # in total, we have 16 tables for sure
                # therefore we could count the table number and decide which port it falls in
                # it's not a very good solution but it's a quick fix
                # given that current structure of the report it's hard for us to identify port
                # name for each table, I think this would be fine until the report changes structure
                raw_item.update(port_name=PORT_NAME[(table_count - 1) // 4])

                yield normalize.process_item(raw_item)
            # in case any row has missing field, so that we can capture
            # skip empty rows
            elif 'NIL' not in row:
                if may_strip(''.join(row)):
                    self.missing_rows.append(' '.join(row))

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
