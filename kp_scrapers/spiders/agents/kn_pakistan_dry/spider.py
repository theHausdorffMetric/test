import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.kn_pakistan_dry import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


class KanooPakistanDry(ShipAgentMixin, MailSpider):
    name = 'KN_Pakistan_Grades_DRY'
    provider = 'Kanoo'
    version = '1.0.0'
    # we want to create portcalls from this commercial report, hence PortCall model is used
    produces = [DataTypes.Vessel, DataTypes.Cargo, DataTypes.PortCall]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """Extract data from mail specified with filters in spider args.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:
        """
        self.reported_date = self.extract_reported_date(mail.envelope['subject']) or to_isoformat(
            mail.envelope['date']
        )

        body = self.select_body_html(mail)
        start_processing = False
        for raw_row in body.xpath('//table//tr'):
            row = [
                may_strip(''.join(cell.xpath('.//text()').extract()))
                for cell in raw_row.xpath('.//td')
                if ''.join(cell.xpath('.//text()').extract())
            ]
            if not row:
                continue

            if 'oil seeds' in row[0].lower():
                start_processing = True
                continue

            if start_processing:
                # full row contains 9 cells
                if len(row) != 9:
                    continue

                if 'ships name' in row[0].lower():
                    header = row
                    continue

                raw_item = {header[cell_idx]: cell for cell_idx, cell in enumerate(row)}
                raw_item.update(provider_name=self.provider, reported_date=self.reported_date)
                yield normalize.process_item(raw_item)

    @staticmethod
    def extract_reported_date(rpt_subject):
        """Get reported date

        Args:
            rpt_subject (str):

        Returns:
            str:
        """
        _match = re.match(r'.*\s(\d{1,2}\.\d{1,2}\.\d{1,4})', rpt_subject)
        if _match:
            return to_isoformat(_match.group(1), dayfirst=True)

        return None
