# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider


MOVEMENT_MAPPING = {'D': 'discharge', 'L': 'load'}


class KanooAlJubailSpider(ShipAgentMixin, PdfSpider, MailSpider):

    name = 'KN_AlJubail'
    provider = 'Kanoo'
    version = '1.0.1'

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """The method will be called for every mail the search_term matched.
        """
        # in case reported date in report has errors
        mail_reported_date = to_isoformat(mail.envelope['date'])

        for attachment in mail.attachments():
            # sanity check, in case there are email header images or other such nonsense
            if not attachment.is_pdf:
                continue

            for idx, row in enumerate(self.extract_pdf_io(attachment.body, **{'-l': []})):
                # each row is laid out as such:
                # [<irrelevant>, <vessel>, <berth>, <product>, <movement>, <quantity>, <matching_date, <irrelevant>] # noqa

                # first row will always contain reported date in the first element
                if idx == 0:
                    reported_date = self._normalize_reported_date(row[0]) or mail_reported_date

                # yield dicts of the data rows
                if row[0] != '' and idx != 0:
                    yield self._build_item(row, reported_date)

    @staticmethod
    def _normalize_reported_date(raw_date):
        """Normalize reported date to ISO-8601 format.

        Dates can come in the following format:
            - '18JULY2017'
            - '18.JULY2017'
            - '18JULY.2017'
            - '18.JULY.2017'
            - '01.JAN.20189'

        Because `to_isoformat` cannot parse dates with no delimiters,
        we preprocess it before sending it to the function.

        Args:
            raw_date (str):

        Returns:
            str: ISO-8601 timestamp

        Examples:
            >> self._normalize_reported_date('18JULY2017')
            u'2017-07-18T00:00:00'
            >> self._normalize_reported_date('18JULY2017')
            u'2017-07-18T00:00:00'
            >> self._normalize_reported_date('18JULY2017')
            u'2017-07-18T00:00:00'
            >> self._normalize_reported_date('18JULY2017')
            u'2017-07-18T00:00:00'
        """
        if re.match(r'(\d{1,2}\.[A-z]+\.\d{4})$', raw_date):
            return to_isoformat(
                re.sub(r'(\d+)\.?([a-zA-Z]+)\.?(\d+)', r'\g<1> \g<2> \g<3>', raw_date),
                dayfirst=True,
            )

    @validate_item(CargoMovement, normalize=True, strict=False)
    def _build_item(self, row, reported_date):
        return {
            'reported_date': reported_date,
            'eta': to_isoformat(row[6]),
            'port_name': 'Al-Jubail',
            'provider_name': self.provider,
            'cargo': {
                'product': row[3],
                'movement': MOVEMENT_MAPPING.get(row[4]),
                'volume': row[5],
                'volume_unit': Unit.kilotons,
            },
            'vessel': {'name': row[1]},
        }
