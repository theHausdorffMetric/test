# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import re
from tempfile import TemporaryFile

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.docx import read_docx_io
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.rn_fixtures import normalize


RELEVANT_ROW_PATTERN = re.compile(
    r'(.+?)\d{2,} +([A-Z+]+)? *(\d{2} ?[a-zA-Z]{3}) +([a-zA-Z+]+( {,2}\w+)?) +([a-zA-Z+]+( {,2}\w{2,})?) +[a-zA-Z0-9./]+ +(.*)'  # noqa
)  # noqa
# extra row data usually begins with at least 3 spaces
RELEVANT_EXTRA_ROW_PATTERN = r' {3,}(\S*)'
YEAR_PATTERN = r'.+\d{4}'
DATE_PATTERN = r'\d{,2}[a-zA-Z]{2} [a-zA-Z]+ \d{4}'


class RNFixtureSpider(CharterSpider, MailSpider):

    name = 'RN_Fixtures'
    provider = 'Galbraith'
    version = '1.0.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """Extract raw data from the given mail.

        Because data may be given in either the email body or a .docx attachment,
        there is a need to dispatch the extracting functions separately.

        Args:
            mail (Mail): see `lib.services.mail.Mail` for details

        Yields:
            Dict[str, str]:
        """
        if list(mail.attachments()):
            for attachment in mail.attachments():
                yield from self.parse_docx(attachment)
        else:
            yield from self.parse_html(mail)

    def parse_docx(self, attachment):
        """Extract raw data from the given .docx attachment.

        Args:
            attachment (Attachment): see `lib.services.mail.Attachment` for details

        Yields:
            Dict[str, str]:
        """
        # sanity check
        if attachment.is_docx:
            # TODO could be encapsulated in a generic
            docx_file = TemporaryFile()
            docx_file.write(attachment.body)

            # docx extraction does not convert non-ascii characters automatically
            raw_rows = self._build_raw_rows(read_docx_io(docx_file))

            # un-parseable docx (due to inconsistent formatting) will not have year info
            if not (re.match(YEAR_PATTERN, raw_rows[0])):
                self.logger.info("Vlcc fixture formatted, can't be parsed.")
                return

            reported_date = normalize.parse_reported_date(raw_rows[0])
            for raw_item in self.parse_raw_rows(raw_rows, reported_date):
                yield raw_item

    def parse_html(self, mail):
        """Extract raw data from the given email body.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:
        """
        response = self.select_body_html(mail)
        raw_rows = self._build_raw_rows(response.xpath('//text()').extract())

        # detect reported date in list and pop the rest in front
        for start in range(len(raw_rows)):
            if re.match(DATE_PATTERN, ''.join(raw_rows[start : start + 3])):
                del raw_rows[:start]

        reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')
        for raw_item in self.parse_raw_rows(raw_rows, reported_date):
            yield raw_item

    def parse_raw_rows(self, rows, reported_date):
        """Parse raw document lines and convert them to a raw dict.

        Args:
            rows (List[str]):

        Yields:
            Dict[str, str]:
        """
        for idx, row in enumerate(rows):
            # next two rows '(FAILED)' indicates the charter failed, can discard current
            if idx < len(rows) - 2 and ('(FAILED)' in rows[idx + 1] or '(FAILED)' in rows[idx + 2]):
                self.logger.info(f'Charter has failed, discarding: {row}')
                continue

            row_match = RELEVANT_ROW_PATTERN.match(row)
            if row_match:
                # discard vessels that are 'To Be Named' aka 'TBN'
                if 'TBN' in row_match.group(1):
                    self.logger.info('At row {}, vessel is TBN, discarding'.format(row))
                    continue
                raw_item = self._build_raw_item(row_match)
            # FIXME regex may fail on unseen edge cases, discard for now
            else:
                self.logger.warning("Row not matched: '{}', row {}".format(row, idx))
                continue

            # update item if subsequent rows have additional arrival zones
            lookahead = 1
            addinfo_pattern = r' {3,}(\S*)'
            while idx < len(rows) - lookahead and re.match(
                RELEVANT_EXTRA_ROW_PATTERN, rows[idx + lookahead]
            ):
                extra_row_match = re.match(addinfo_pattern, rows[idx + lookahead]).group(1)
                raw_item['arrival_zone'] = raw_item['arrival_zone'] + '-' + extra_row_match
                lookahead += 1

            # additional meta fields not found in document
            raw_item.update(provider_name=self.provider, reported_date=reported_date)
            yield normalize.process_item(raw_item)

    @staticmethod
    def _build_raw_item(row_match):
        """Build raw dict item from a regex match.

        Example row:
        STENA IMPERATOR    37  CLN    03 Jun  BALTIC           USA          W140     VITOL # noqa

        Failure to match usually occurs when Arrival Zone and Departure Zone are too close in spacing.
        Logger will log every Non match row

        re groups explanation
        ======================

        1st group, Vessel Name (.+?): STENA IMPERATOR
        Matches everything until the first occurence of 2 digits, the DWT which is not needed

        2nd Group, Cargo ([A-Z+]+)?: CLN
        Row may or may not have cargo information

        3rd Group, Laycan Date (\d{2} ?[a-zA-Z]{3}): 03 Jun
        Laycan date may or may not have spacing between day and month
        Day is always 2 digits, month is always 3 characters

        4th Group +5th, Departure Zone ([a-zA-Z+]+( {,2}\w+){,2}): BALTIC
        May have up to 2 spaces between each part of the name

        6th + 7th Group, Arrival Zone ([a-zA-Z+]+( {,2}\w{2,}){,2}): USA
        Same as above, except additional arrival zones may be included in the next row.

        8th Group, Charterer Name (\w+): VITOL
        May be 1 or 2 words. 8th group will not match the full charterer name if it is 2 words,
        but this method is less error prone then trying to match by spaces.
        """
        return {
            'vessel_name': row_match.group(1),
            'cargo': row_match.group(2),
            'lay_can_start': row_match.group(3),
            'departure_zone': row_match.group(4),
            'arrival_zone': row_match.group(6),
            'charterer': row_match.group(8),
        }

    @staticmethod
    def _build_raw_rows(rows):
        """Replaces Non-ASCII characters from list of rows

        Args:
            List[str]:

        Returns:
            List[str]:
        """
        return [x.replace('\xa0', ' ').replace(u'\u2019', "'") for x in rows]
