from scrapy import Request

from kp_scrapers.lib.parser import serialize_response
from kp_scrapers.spiders.bases.markers import OilMarker
from kp_scrapers.spiders.bases.persist import PersistSpider
from kp_scrapers.spiders.contracts import ContractSpider
from kp_scrapers.spiders.contracts.eia_flows import normalize


class EiaFlowsSpider(OilMarker, ContractSpider, PersistSpider):
    name = 'EIA_Flows'
    provider = 'EIA'
    version = '2.0.0'
    # TODO use a Flows model for validation and robustness

    start_urls = ['http://api.eia.gov/series/?api_key={token}&series_id={series}{kwargs}']

    def __init__(self, token, start_date=None, end_date=None, *args, **kwargs):
        # get persistence state
        super().__init__(*args, **kwargs)
        self.dates_extracted = self.persisted_data.get('dates_extracted', {})

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

        for each in normalize.SERIES_FLOW_MAPPING:
            url = self.start_urls[0].format(token=self.token, series=each, kwargs=params)
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

        # check persistence if series data has been extracted already
        if not self._check_persistence(raw_series):
            return

        # yield items point-by-point from the series
        for datapoint in raw_series.pop('data'):
            raw_item = raw_series.copy()
            raw_item.update(zip(('x_value', 'y_value'), datapoint))

            # contextualise raw item with some meta info
            raw_item.update(provider_name=self.provider)
            yield normalize.process_item(raw_item)

    def _check_persistence(self, series):
        """Check if the series data received contains data we've already scraped before.

        FIXME workaround for the fact that the ExternalFlows table cannot filter and discard
        duplicate items that we scrape hourly. This is made complicated by the fact there is
        no pre-determined time when the source will update its data, hence we scrape hourly.

        Args:
            series (Dict[str, Any]):

        Returns:
            bool: True if data has not been scraped before
        """
        # check if data has been extracted already
        if series['end'] in self.dates_extracted.get(series['series_id'], []):
            self.logger.info(
                f'Data already extracted previously: {series["series_id"]} ({series["end"]})'
            )
            return False

        # save persistence state
        self.dates_extracted.setdefault(series['series_id'], []).append(series['end'])
        self.persisted_data.update(dates_extracted=self.dates_extracted)
        self.persisted_data.save()

        return True
