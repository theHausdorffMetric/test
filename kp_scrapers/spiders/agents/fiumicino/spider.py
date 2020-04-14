import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.fiumicino import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


class FiumicinoSpider(ShipAgentMixin, MailSpider):
    name = 'Fiumicino_Grades'
    provider = 'Nolarma'
    version = '1.0.0'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        '''The method will be called for every mail the search_term matched.
        The cargo movement in this mail is semi structured so we need to use
        state to parse the mail:
        A new item is created when there is a line (starting with 'mt' or 'm/t')
        of the forme 'mt(or m/t) ...'
        This new item is  process when and only when the next line starting with 'ops' is found
        example :
        SPM R1
        ------

        M/T PACIFIC NAFSIKA

        ARRIVED:25/02 2230

        ETB:N/A

        ETC:N/A

        ETS:N/A

        OPS: DISCHARGING ULSD ABT 95000 MT



        SPM R2

        ------

        M/T CALAIUNCO

        ARRIVED:26/0902

        ETB:N/A

        ETC:N/A

        ETS:N/A

        OPS:DISCH ABT MT 17000 BIODIEEL

        ========


        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:

        '''
        # memoise and use received timestamp of email as default reported date
        self.reported_date = to_isoformat(mail.envelope['date'])

        split_keyword = 'eta|etb|etc|ets|ops|sailed|arrived|berthed|new etb'

        raw_rows = [
            row.lower()
            for row in self.select_body_html(mail).xpath('//text()').extract()
            if may_strip(row)
        ]

        start_processing = False
        is_cargo_movement = False

        for idx, row in enumerate(raw_rows):
            row = may_strip(row)

            # start of relevant data
            if row.find('spm r1') != -1:
                start_processing = True
                berth = 'spm r1'

            # update berth information
            if row.find('spm r2') != -1:
                berth = 'spm r2'

            # if start_processing = False, it means we are at the beginning of the mail where
            # information is not relevant
            if not start_processing:

                continue

            # 'MT' or 'M/T' indicate it's the line with the name of the vessel, the line bellow
            # will indicate the other information relative to the cargo movement associated
            if row.startswith(('mt', 'm/t')):
                raw_item = {}
                is_cargo_movement = True
                raw_item['vessel'] = re.split('mt|m/t', row, maxsplit=1)[1]
                continue

            if not is_cargo_movement:
                continue

            # try split on : or on keyword if : are absent
            row_data = row.split(':')
            if len(row_data) > 1:
                raw_item[row_data[0]] = row_data[1]
            else:
                row_data = re.split(split_keyword, row, maxsplit=1)
                if len(row_data) > 1:
                    raw_item[re.search(split_keyword, row).group(0)] = row_data[1]

            if row.startswith('ops'):
                is_cargo_movement = False
                raw_item.update(
                    port_name='fiumicino',
                    provider_name=self.provider,
                    reported_date=self.reported_date,
                    berth=berth,
                )
                yield from normalize.process_item(raw_item)
