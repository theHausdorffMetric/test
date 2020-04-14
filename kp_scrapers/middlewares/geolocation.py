from functools import partial
import logging

import requests
from scrapy.exceptions import NotConfigured


logger = logging.getLogger(__name__)


# NOTE ipdata.co has a free tier limit of 1000 requests per day
GEOLOCATION_API = 'https://api.ipdata.co/'


class GeolocationMiddleware:
    """Validates geographical location from requests, specified with `GEOLOCATION_*` settings.

    Settings:
        GEOLOCATION_ENABLED (Union[bool, str]): enable/disable geolocation check
        GEOLOCATION_STRICT (Union[bool, str]): whether to be strict about geolocation checks
        GEOLOCATION_CITY (str): formatted as "<CITY_NAME>, <ISO3166-2_COUNTRY_CODE>"
        GEOLOCATION_API_KEY (str): auth key for external geolocation service

    """

    def __init__(self, token, strict, location):
        # settings for api functionality
        self.token = token
        self.strict = strict

        # city in which the spider should be running
        # format: "<CITY_NAME>, <ISO3166-2_COUNTRY_CODE>"
        self.location = location

        # reuse TCP connection for better performance
        self._session = requests.Session()
        self.api = partial(self._session.get, GEOLOCATION_API)

    @classmethod
    def from_crawler(cls, crawler):
        strict = str(crawler.settings.get('GEOLOCATION_STRICT')).lower() == 'true'
        is_enabled = str(crawler.settings.get('GEOLOCATION_ENABLED')).lower() == 'true'

        if not is_enabled:
            raise NotConfigured("`GEOLOCATION_ENABLED` is false")

        # it does not make sense to carry on if we don't specify the location and api key
        # to validate the spider's IP address with
        token = crawler.settings.get('GEOLOCATION_API_KEY')
        location = crawler.settings.get('GEOLOCATION_CITY')
        if not location or not token:
            # CloseSpider cannot be used with DownloaderMiddleware as of 6 September 2019
            # see https://github.com/scrapy/scrapy/issues/2578
            raise ValueError("GEOLOCATION_ENABLED, however no api key and/or city specified")

        return cls(token, strict, location)

    def process_request(self, request, spider):
        response = self.api(**self._build_api_options(self.token, request)).json()
        valid_location = self._is_valid_location(response)
        logger.debug('IP address %s (%s)', response.get('ip'), self._response)

        # business as usual: geolocation check passed, or non-strict validation
        if valid_location or (not valid_location and not self.strict):
            # we don't return anything so all other processing steps continue as is
            return

        # validation failed, however non-strict mode. spider will continue
        if not self.strict:
            logger.error(
                'Expected location "%s", however obtained "%s"', self.location, self._response
            )
            return

        # validation failed and strict mode
        # CloseSpider cannot be used with DownloaderMiddleware as of 6 September 2019
        # see https://github.com/scrapy/scrapy/issues/2578
        raise ValueError(
            f'Expected location "{self.location}", however obtained "{self._response}"'
        )

    def _is_valid_location(self, response):
        # memoise response, formatted as "<CITY_NAME>, <ISO3166-2_COUNTRY_CODE>"
        self._response = f'{response.get("city")}, {response.get("country_code")}'

        _city, _, _country = tuple(s.strip() for s in self.location.rpartition(','))
        return _city == response.get('city') and _country == response.get('country_code')

    def _build_api_options(self, api_key, request):
        # init auth form first
        options = {'params': {'api-key': api_key}}

        # NOTE for this to work as intended,
        # GeolocationMiddleware needs to be loaded later than ProxyMiddleware
        proxy = request.meta.get('proxy')
        if proxy:
            # NOTE protocol list is non-exhaustive
            options['proxies'] = {'http': proxy, 'https': proxy, 'ftp': proxy}

        return options
