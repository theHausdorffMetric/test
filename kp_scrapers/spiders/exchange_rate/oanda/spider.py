from datetime import datetime, timedelta
import json
import logging
import urllib

from scrapy.http import Request

from kp_scrapers.constants import BLANK_START_URL
from kp_scrapers.models.exchange_rate import ExchangeRate
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.exchange_rate import ExchangeRateSpider


BASE_CURRENCY = 'USD'

CURRENCY_TO_CONVERT_TO = ['EUR', 'NOK', 'QAR', 'USD', 'GBP', 'AUD']

logger = logging.getLogger(__name__)


class ExchangeRateSpider(ExchangeRateSpider):
    name = 'ExchangeRate'
    version = '1.1.0'
    provider = 'OANDA'
    produces = [DataTypes.ExchangeRate]

    start_urls = [BLANK_START_URL]

    def __init__(self, start_date=None, end_date=None):
        # why is the start_date 2 days behind the current date?
        # the api gives the date info for two days older dates only. So to safegurd things we have
        # added to extract last two dates.
        # Even if the crawler scrapes redundant dates, the loader will not allow duplicate
        # record insertion
        self.start_date = (
            start_date if start_date else (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        )
        self.end_date = datetime.now().strftime('%Y-%m-%d')
        self.url = 'http://www.oanda.com/fx-for-business/historical-rates/api/data/update/?'

    @property
    def build_params(self):
        return {
            'source': 'OANDA',
            'adjustment': 0,
            'period': 'daily',
            'price': 'mid',
            'view': 'table',
            'start_date': self.start_date,
            'end_date': self.end_date,
            'base_currency': BASE_CURRENCY,
        }

    def parse(self, _):
        """Entry point for Exchange rate spider

        Args:

        Yields:
            Dict[str, Any]:
        """
        params = self.build_params
        params.update(
            {
                'quote_currency_{}'.format(index): curr
                for index, curr in enumerate(CURRENCY_TO_CONVERT_TO)
            }
        )

        yield Request(
            url=self.url + urllib.parse.urlencode(params), callback=self.parse_exchange_rates
        )

    def parse_exchange_rates(self, response):
        """Parse the JSON response returned by the OANDA source

        Sample Response
        ---------------
        {
            'frequency': 'daily',
            'ui': {
                'max_period': 0,
                'min_period': 0,
            },
            'widget': [{
                'average': '0.869128',
                'baseCurrency': 'USD',
                'data':[
                    [1540166400000, '0.870067'],
                    [1540080000000, '0.868190'],
                    # up to 60 days of rates
                    ...
                ],
                'high': '0.870067',
                'low': '0.868190',
                'quoteCurrency': 'EUR',
                'type': 'bid',
            }],
        }
        """

        json_obj = json.loads(response.body_as_unicode())
        currencies_data = json_obj.get('widget')

        if not currencies_data:
            logger.error(
                'Api might have changed, need to Investigate the API responses',
                extra={'response': json_obj},
            )
            return

        for currency_data in currencies_data:
            if 'data' in currency_data:
                yield from self.format_api_data(
                    currency_data['quoteCurrency'], currency_data['data']
                )

    @validate_item(ExchangeRate, normalize=True, strict=True, log_level='error')
    def format_api_data(self, currency_code, api_timeseries):
        for timestamp, rate in api_timeseries:
            yield {
                'date_utc': datetime.utcfromtimestamp(timestamp / 1000.0).isoformat(),
                'rate': float(rate),
                'currency_code': currency_code,
                'provider_name': self.provider,
            }
