from collections import namedtuple
from datetime import datetime
import re

from dateutil.parser import parse as parse_date
from six.moves import zip

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.items import StockExchangeIndex
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.prices import PriceSpider


INDEX_COMMODITY = 'lng'
PROVIDER_NAME = 'EMC'
UNIT = 'usd/mmbtu'

GEO_SUBINDEX = namedtuple('geo_subindex', 'index_name geo_zone base_ticker')
# Should exactly match zones in database
SLING_ZONES = (
    GEO_SUBINDEX('SGP SLInG', 'Singapore Republic', 'SGPSLInG'),
    GEO_SUBINDEX('N.Asia SLInG', 'Russian Federation', 'NAsiaSLInG'),
    GEO_SUBINDEX('DKI SLInG', 'Middle East', 'DKISLInG'),
)

DATE_REGEX = 'Please find below the Sling Index values on (\d\d\s\S\S\S\s\d\d\d\d)'
SLING_REGEX = r'Index \(([\S]+)\)\s+(\d+\.\d+)'
H_SLING_REGEX = r'(H\d\s\S\S\S)\s+(\d+\.\d+)'


class SlingSpider(PriceSpider, MailSpider):
    name = 'SLING'
    version = '2.0.0'
    provider = 'SLInG'
    produces = []

    reported_date = None

    def parse_mail(self, mail):
        """Entry point of SLING spider for DKI, North Asia ans Singapore stock exchange index.

        Args:
            mail (Mail):

        Returns:

        """
        body = self.select_body_html(mail)
        table = may_strip(' '.join(body.xpath('//text()').extract()))
        reported_date = parse_date(mail.envelope['date'])

        yield from self.get_sling_from_text(table, reported_date)

    @staticmethod
    def get_sling_from_text(file_as_text, publication_date):
        date_ = parse_date(re.findall(DATE_REGEX, file_as_text).pop())
        indices, h_figures = list(zip(*re.findall(H_SLING_REGEX, file_as_text)))
        halfs, indices = list(zip(*enumerate(indices)))
        # formated_halfs should be between N1 and N4 to match ticker enum
        # Example: ticker == 'SGPSLInGNx'
        formated_halfs = ['N' + str(half % 4 + 1) for half in halfs]

        indice_values, indice_occurs = [], []
        for i in indices:
            indice_occurs.append(indice_values.count(i))
            indice_values.append(i)

        subindexes = [SLING_ZONES[i] for i in indice_occurs]
        dates_ = [SlingSpider._parse_date_from_half_month(date_, i) for i in indices]
        h_sling_figures = list(zip(formated_halfs, subindexes, dates_, h_figures))

        for half, subindex, month, figure in h_sling_figures:
            yield StockExchangeIndex(
                raw_unit=UNIT,
                raw_value=figure,
                converted_value=figure,
                index=subindex.index_name,
                ticker=subindex.base_ticker + half,
                commodity=INDEX_COMMODITY,
                zone=subindex.geo_zone,
                provider=PROVIDER_NAME,
                month=datetime.strftime(month, '%Y-%m-%d'),
                day=datetime.strftime(publication_date, '%Y-%m-%d'),
            )

        for i, (month_literal, figure) in enumerate(re.findall(SLING_REGEX, file_as_text)):
            month = publication_date.replace(month=datetime.strptime(month_literal, '%B').month)
            yield StockExchangeIndex(
                raw_unit=UNIT,
                raw_value=figure,
                converted_value=figure,
                index=SLING_ZONES[i].index_name,
                ticker=SLING_ZONES[i].base_ticker + 'Index',
                commodity=INDEX_COMMODITY,
                zone=SLING_ZONES[i].geo_zone,
                provider=PROVIDER_NAME,
                month=datetime.strftime(month, '%Y-%m-01'),
                day=datetime.strftime(publication_date, '%Y-%m-%d'),
            )

    @staticmethod
    def _parse_date_from_half_month(date_, indice):
        half, raw_month = indice.split(' ')
        month = datetime.strptime(raw_month, '%b').month
        day = 1 if half == 'H1' else 15
        return datetime(date_.year if month > date_.month else date_.year + 1, month, day)
