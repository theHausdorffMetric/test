"""Equasis Spider
   ==============

website: http://www.equasis.org/EquasisWeb/public/HomePage


## Usage:

    $ alias scrape='scrapy crawl --loglevel DEBUG -o - -t jl'
    $ scrape Equasis \
        -a min_year=2016 \
        -a max_year=2017 \
        -a category=6 \
        -a min_page=1 \
        -a max_page=5

    # To do a test run, add the `test` argument
    $ scrape Equasis \
        -a test=1 \
        -a min_year=2016 \
        -a max_year=2017

    # If the persistent storage is corrupted, you can delete it by using `delete_files`
    $ scrape Equasis \
        -a delete_files=1 \
        -a min_year=2016 \
        -a max_page=3

## Deployment on ETL:

    $ export SH_PROJECT_ID=434 ; kpler_extract "equasis -s -l" --no-lock

"""
from abc import abstractmethod
import datetime
import logging

from scrapy.exceptions import CloseSpider

from kp_scrapers.lib.static_data import fetch_kpler_fleet
from kp_scrapers.spiders.bases.persist import PersistSpider
from kp_scrapers.spiders.registries import RegistrySpider
from kp_scrapers.spiders.registries.equasis import constants
from kp_scrapers.spiders.registries.equasis.login import Credentials
from kp_scrapers.spiders.registries.equasis.session import SearchSession, VesselSession


logger = logging.getLogger(__name__)

# NOTE probably worth factorizing this elsewhere
# NOTE arbitrary value, find something wiser
SHUB_MEMORY_TOLERANCE = 900000000
NORMAL_EXIT_REASON = 'finished'


class _EquasisCrawler(RegistrySpider, PersistSpider):
    """Base crawl class for Equasis."""

    name = '_EquasisCrawler'
    version = '1.1.0'
    provider = 'Equasis'

    spider_settings = {
        'DATADOG_CUSTOM_METRICS': [
            # we usually want to monitor the quantity of data found and the performance
            'item_scraped_count',
            'response_received_count',
            # logs can be a good indicator of something going wrong
            'log_count/WARNING',
            'log_count/ERROR',
            # custom debugging
            'equasis/pages_count',
            'equasis/requests_failed',
            'equasis/logins',
            # equasis spider has the bad idea to often fail on too much memory used
            'memusage/max',
        ]
    }

    # NOTE at this point it's almost ready to be shared accross every spider
    def handle_high_memory(self, data, tolerance=SHUB_MEMORY_TOLERANCE):
        memusage = self.crawler.stats.get_value('memusage/max')
        if memusage > tolerance:
            self.logger.warning('reaching memory limit: {}'.format(memusage))
            # TODO if near max memory usage : store imos and close spiders
            self.persisted_data.update(data)
            self.persisted_data.save()

            # gracefully end spider (finished being the normal code)
            raise CloseSpider(NORMAL_EXIT_REASON)

    @abstractmethod
    def parse_vessels(self, imo_list):
        """Mandatory method called after imos are aggregated."""
        return

    @abstractmethod
    def make_credentials(self):
        return


class EquasisSpider(_EquasisCrawler):
    """We crawl using the search per vessel category.

    We could also use direct GET url:
        http://www.equasis.org/EquasisWeb/restricted/ShipInfo?fs=ShipList&P_IMO=9772711

    But it only works for vessel we already know, not for new ones or when we
    load a new vessel category for new commodity.  Maybe we can combine the 2
    methods

    """

    name = 'Equasis'
    version = '1.1.0'
    provider = 'Equasis'

    def __init__(self, *args, **kwargs):
        """Initialize Equasis vessel filters.

        Notes:
            Scrapinghub returns arguments as strings, we have to cast them to their appropriate
            types here.

        Args:
            min_page (str): min page of results
            max_page (str): max page of results
            min_year (str): min build year of vessel
            max_year (str): max build year of vessel
            imos (str): comma-delimited list of IMOs to get data for
            category (str): vessel category, see `constants.py` for details
            filters (str): comma-delimited key-value pairs, functions like kwargs

        """
        super(EquasisSpider, self).__init__(*args, **kwargs)

        self._is_test = kwargs.get('test') is not None

        # renew credentials for every N vessels parsed
        # this is done to distribute downloads across all logins and minimise possibility of bans
        self.search_quota = int(kwargs.get('vessels_per_login', 5))

        # be gentle with equasis: split search
        self._min_page = int(kwargs.get('min_page', constants.DEFAULT_MIN_PAGE))
        self._max_page = int(kwargs.get('max_page', constants.DEFAULT_MAX_PAGE))

        self._max_year = int(kwargs.get('max_year', datetime.datetime.now().year))
        self._min_year = int(kwargs.get('min_year', self._max_year))

        # one can force the imo scrapped instead of searching everything
        last_run_imos = self.persisted_data.get('imos_queue')
        self._requested_imos = kwargs.get('imos', last_run_imos)
        if isinstance(self._requested_imos, str):
            self._requested_imos = self._requested_imos.split(',')
        # reset queue so that we don't load it next run
        self.persisted_data.pop('imos_queue', None)

        category = kwargs.get('category')
        _categories = [x for x in constants.VESSEL_CATEGORIES if x['code'] == category]
        self._categories = _categories or constants.VESSEL_CATEGORIES

        # extras control for ad-hoc filtering
        # one must provide it at the cli like 'key:value,foo:bar'
        # TODO support for human mapping copied on Equasis
        filters = kwargs.get('filters')
        self.extra_filters = (
            {arg.split(':')[0]: arg.split(':')[1] for arg in filters.split(',')} if filters else {}
        )

        logger.info('filtering search on categories {}'.format(self._categories))
        logger.info('will scrape from page {} to {}'.format(self._min_page, self._max_page))
        logger.info('and from {} to {}'.format(self._min_year, self._max_year))
        if self._is_test:
            logger.info('test run, using dev logins')

    def start_requests(self):
        """Entry point for Equasis spider.

        Returns:
            scrapy.Request:

        """
        if self._requested_imos:
            logger.info('Scraping given imos: {}'.format(self._requested_imos))
            return [self.parse_vessels(self._requested_imos)]

        else:

            session = SearchSession(self, self.make_search_params())
            return session.open_session()

    def parse_vessels(self, imo_list):
        # NOTE since too many imos will make the spider to crash, may be we
        # could die early here instead
        session = VesselSession(self, imo_list)
        return session.open_session()

    def make_credentials(self):
        return Credentials(
            persisted_data=self.persisted_data,
            inventory=constants.DEV_USERS if self._is_test else constants.PROD_USERS,
            stats=self.crawler.stats,
        )

    def make_search_params(self):
        """See the mapping from PARAMS_EQUASIS_MAPPING in api.py."""
        params_list = []
        for category in self._categories:
            params = {
                'ship_category': category['code'],
                'min_build_year': self._min_year,
                'max_build_year': self._max_year,
                'min_page': self._min_page,
                'max_page': self._max_page,
                'extra_filters': self.extra_filters,
            }
            params_list.append(params)

        return params_list


class EquasisActiveSpider(EquasisSpider):
    """We crawl using the search per vessel category.

    We could also use direct GET url:
        http://www.equasis.org/EquasisWeb/restricted/ShipInfo?fs=ShipList&P_IMO=9772711

    But it only works for vessel we already know, not for new ones or when we
    load a new vessel category for new commodity.  Maybe we can combine the 2
    methods

    """

    name = 'EquasisActive'
    version = '1.0.0'
    # provider should actually be Equasis, but we mock Gibson so that the loaders
    # won't reject the data due to lower provider confidence by Equasis
    provider = 'GR'

    def __init__(self, *args, **kwargs):
        """Initialize EquasisActive filters.

        Notes:
            Scrapinghub returns arguments as strings, we have to cast them to their appropriate
            types here.

        Args:
            whitelist (str): comma-delimited list of fields whitelisted in the final item
            blacklist (str): comma-delimited list of fields blacklisted in the final item

        """
        super().__init__(*args, **kwargs)

        self.whitelist = set(self._job_kwargs_list(kwargs, 'whitelist'))
        self.blacklist = set(self._job_kwargs_list(kwargs, 'blacklist'))

    def _job_kwargs_list(self, kwargs, field):
        return (field.strip() for field in kwargs.get(field, '').split(',') if field.strip())

    def start_requests(self):
        """Entry point for Equasis spider."""

        # get all active vessels to be scraped
        self._requested_imos = list(
            v['imo']
            for v in fetch_kpler_fleet(
                is_eligible=lambda v: v['status'] == 'Active', disable_cache=True
            )
        )

        logger.info('Going to scrape for %d active vessels' % len(self._requested_imos))
        yield self.parse_vessels(self._requested_imos)

    def parse_vessels(self, imo_list):
        # NOTE since too many imos will make the spider to crash,
        # maybe we could die early here instead
        session = VesselSession(self, imo_list, whitelist=self.whitelist, blacklist=self.blacklist)
        return session.open_session()
