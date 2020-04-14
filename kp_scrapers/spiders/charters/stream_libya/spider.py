from dateutil.parser import parse as parse_date
from w3lib.html import remove_tags

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.stream_libya import normalize_charters, normalize_grades


class StreamLibyaSpider(CharterSpider, MailSpider):

    name = 'STM_Libya'
    provider = 'Stream Ships'
    version = '0.2.1'

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
        'MARK_MAIL_AS_SEEN': False,
    }

    def parse_mail(self, mail):
        """Extract data from mail specified with filters in spider args.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:
        """
        # source doesn't provide `reported_date` in email body; we guess it from mail headers
        reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')
        if 'part 2' not in mail.envelope['subject'].lower():

            tables = self.select_body_html(mail).xpath('//table')
            for tbl in tables:
                for idx, raw_row in enumerate(tbl.xpath('.//tr')):
                    row = [remove_tags(cell) for cell in raw_row.xpath('./td').extract()]

                    # discard malformed rows
                    if len(row) < 7:
                        continue

                    if 'Terminal' in row[1]:
                        header = row
                        continue

                    raw_item = {may_strip(header[idx]): cell for idx, cell in enumerate(row)}

                    raw_item.update(provider_name=self.provider, reported_date=reported_date)
                    if DataTypes.SpotCharter in self.produces:
                        yield normalize_charters.process_item(raw_item)
                    if DataTypes.Cargo in self.produces:
                        yield from normalize_grades.process_item(raw_item)

    @property
    def missing_rows(self):
        return normalize_charters.MISSING_ROWS


class StreamLibyaFixturesSpider(StreamLibyaSpider):
    name = 'STM_LibyaFixtures_OIL'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
        # prevent spider from marking the email as seen
        # so that multiple spiders can run on it
        'MARK_MAIL_AS_SEEN': False,
    }


class StreamLibyaGradesSpider(StreamLibyaSpider):
    name = 'STM_LibyaGrades_OIL'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
