from datetime import datetime

from dateutil.parser import parse as parse_date

from kp_scrapers.models.items import StockExchangeIndex
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.prices import PriceSpider


INDEX_COMMODITY = 'lng'
INDEX_NAME = 'RIM NE Asia'
TICKER = 'RIM'
PROVIDER_NAME = 'rim-intelligence'
UNIT = 'usd/mmbtu'
ZONE_NAME = 'Japan'

RIM_INDEX = 'RIM Index'


class RIMSpider(PriceSpider, PdfSpider, MailSpider):
    name = 'RIM'
    version = '2.0.0'
    provider = 'RIM'
    produces = []

    reported_date = None

    tabula_options = {'--guess': [], '--pages': ['2'], '--lattice': []}

    def parse_mail(self, mail):
        """Entry point for Pacific RIM stock exchange index spider.

        Args:
            mail:

        Returns:

        """
        for attachment in mail.attachments():
            if not attachment.is_pdf:
                continue

            self.reported_date = parse_date(mail.envelope['date'])
            rim = self.get_price_from_pdf(attachment.body)

            if not rim:
                self.logger.error(
                    f'Could not parse price from file, reported date: {self.reported_date}'
                )
                continue

            # Fill item
            yield StockExchangeIndex(
                raw_unit=UNIT,
                raw_value=rim,
                converted_value=rim,
                index=INDEX_NAME,
                ticker=TICKER,
                commodity=INDEX_COMMODITY,
                zone=ZONE_NAME,
                provider=PROVIDER_NAME,
                month=datetime.strftime(self.reported_date, '%Y-%m-01'),
                day=datetime.strftime(self.reported_date, '%Y-%m-%d'),
            )

    def get_price_from_pdf(self, body):
        """Convert attachment body to pdf, and get price from the file.

        Converted pdf would be like:
        ['', '', '', '', 'Feb 1H', '', '', 'Feb 2H', '', '', 'Mar 1H', '', '', 'RIM Index', '', '', '', '', '', '', '']  # noqa
        ['', '--NEA', '', '', '8.35-8.65', '', '', '8.35-8.65', '', '', '7.95-8.25', '', '', '8.37', '', '', '', '', '', '', '']  # noqa

        First we identify `RIM Index`, and then use the index to retrieve the value in the
        following row.

        Args:
            body:

        Returns:

        """
        table = self.extract_pdf_io(body, **self.tabula_options)

        idx = None
        for row in table:
            if RIM_INDEX in row:
                idx = row.index(RIM_INDEX)
                continue

            if idx:
                return float(row[idx])
