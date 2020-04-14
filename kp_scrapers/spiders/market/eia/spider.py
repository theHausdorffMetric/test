from scrapy import Request, Spider

from kp_scrapers.lib.parser import serialize_response
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.market import MarketSpider
from kp_scrapers.spiders.market.eia import normalize


class EiaMarketFigures(MarketSpider, Spider):
    name = 'EIA_Market_Figures'
    provider = 'EIA'
    version = '1.1.2'
    produces = [DataTypes.MarketFigure]

    start_urls = ['http://api.eia.gov/series/?api_key={token}&series_id={series}{kwargs}']

    def __init__(self, token, start_date=None, end_date=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.token = token
        self._start_param = f'&start={start_date}' if start_date else ''
        self._end_param = f'&end={end_date}' if end_date else ''

    def start_requests(self):
        """Entrypoint of EIA flows spider.

        Default behaviour is to always scrape the most recent series datapoints from API.
        If a start/end date is provided, it is reasonable to assume we want to scrape the entire
        date range, hence we do not want to restrict the quantitiy of returned datapoints.

        Yields:
            scrapy.Request:
        """
        # customise additional query params depending on given spider args
        if self._start_param or self._end_param:
            params = self._start_param + self._end_param
        else:
            params = '&num=1'

        for each in normalize.MARKET_FIGURE_MAPPING:
            args = params
            for key, value in normalize.CATEGORY_MAPPING.items():
                if each in value:
                    args = params + '&category={}'.format(key)
                    break

            url = self.start_urls[0].format(token=self.token, series=each, kwargs=args)
            yield Request(url=url, callback=self.parse_response)

    @serialize_response('json')
    def parse_response(self, response):
        """Parse JSON response from EIA API endpoint.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:
        """
        # sanity check, in case of erroneous response (source will give HTTP 200 regardless)
        if response.get('data', {}).get('error'):
            self.logger.error(response['data']['error'])
            return

        raw_series = response['series'][0]

        # yield items point-by-point from the series
        for datapoint in raw_series.pop('data'):
            raw_item = raw_series.copy()
            raw_item.update(zip(('x_value', 'y_value'), datapoint))

            # contextualise raw item with some meta info
            raw_item.update(provider_name=self.provider)
            yield from normalize.process_item(raw_item)
