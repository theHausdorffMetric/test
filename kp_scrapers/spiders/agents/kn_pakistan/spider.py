from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.kn_pakistan import normalize
from kp_scrapers.spiders.bases.mail import MailSpider


class KanooPakistan(ShipAgentMixin, MailSpider):
    name = 'KN_Pakistan_Grades'
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
        # source doesn't provide `reported_date` in email body; we guess it from mail headers
        reported_date = to_isoformat(mail.envelope['date'])

        raw_rows = [
            row for row in self.select_body_html(mail).xpath('//text()').extract() if may_strip(row)
        ]
        _do_process = False
        port_name = None
        for idx, row in enumerate(raw_rows):

            # start of relevant table data
            if row.startswith('======'):
                _do_process = True
                continue

            # end of relevant table data
            if 'GOVT/PRIORITY SHIPS' in row:
                break

            # don't provess irrelevant rows
            if not _do_process:
                continue

            # memoise port name, since it's not part of the table
            if row.startswith('------'):
                port_name = raw_rows[idx - 1]
                continue

            # process actual data rows
            row = [may_strip(elem) for elem in row.split('\xa0') if may_strip(elem)]
            if len(row) <= 4:
                continue

            raw_item = {str(cell_idx): cell for cell_idx, cell in enumerate(row)}
            raw_item.update(
                port_name=port_name, provider_name=self.provider, reported_date=reported_date
            )
            yield from normalize.process_item(raw_item)
