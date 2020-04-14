import logging

from scrapy.exceptions import CloseSpider
from scrapy.http import FormRequest
from scrapy.spidermiddlewares.httperror import HttpError

from kp_scrapers.lib.utils import random_delay
from kp_scrapers.spiders.registries.equasis import api, constants, parser, utils
from kp_scrapers.spiders.registries.equasis.login import LoginPage


logger = logging.getLogger(__name__)


class EquasisSession(object):
    """Basic session class providing common session behaviour for Equasis"""

    def __init__(self, spider, session_name):
        self.name = session_name
        self.id = None
        self.spider = spider
        self.credentials = self.spider.make_credentials()
        self.search_count = 0

    def start(self, callback):
        """Start a session, login to Equasis website.

        Args:
            callback: triggered after successfully login

        Returns:

        """
        self.id = utils.session_id()
        self.search_count = 0

        logger.info(f'Opening {self.name} [session_id={self.id}]')

        login_page = LoginPage(session=self.id, credentials=self.credentials, on_login=callback)

        return login_page.submit()

    def rotate(self, callback):
        """Rotate credential.

        Args:
            callback: triggered after rotating

        Returns:

        """
        logger.info(f'Rotating {self.name} [session_id={self.id}]')
        return self.start(callback)

    def stay_or_rotate(self, callback):
        """Using current credential or rotate depending on search count.

        Args:
            callback: triggered after rotation.

        Returns:

        """
        # rotate and spread usage across all credentials to minimise chances of being banned
        if self.search_count < self.spider.search_quota:
            self.search_count += 1

        else:
            self.rotate(callback=callback)

    def is_blocked(self, response):
        """Check if current credential is blocked based on response.

        Args:
            response (scrapy.Response):

        Returns:
            Boolean:

        """
        is_blocked, msg = api.is_blocked(response)
        if is_blocked:
            logger.warning(
                f'Current credential is blocked '
                f'[session_name={self.name} '
                f'session_id={self.id} '
                f'msg={msg}]'
            )
            self.credentials.ban()
            return True

        return False

    def has_no_result(self, response):
        """Check if the response contains searching result.

        Args:
            response (scrapy.Response):

        Returns:
            Boolean:

        """
        if api.has_no_results(response.body):
            logger.warning(f'No search results [session_name={self.name} session_id={self.id}]')
            return True

        return False

    def should_retry_when_failed(self, error):
        """Check failed request error type and decide if retry or not.

        Args:
            error:

        Returns:
            Boolean: retry or not

        """
        self.spider.crawler.stats.inc_value('equasis/requests_failed')
        logger.warning(
            f'Vessel request failed, retrying (err={error}) '
            f'[session_name={self.name} '
            f'session_id={self.id}]'
        )

        if error.check(HttpError):
            logger.warning(f'HttpError, skipping current vessel [session_id={self.id}]')
            return False

        logger.info(f'Retrying current vessel [session_id={self.id}]')
        return True


class SearchSession(object):
    """SearchSession for Equasis website.

    This session should be able to:
        - redo searches without changing credentials
        - rotate credentials after the search count has reached search quota
        - restart, skip current page or search parameters if failure

    """

    def __init__(self, spider, search_filters):
        self.session = EquasisSession(spider, self.__class__.__name__)
        self.search_filters = search_filters

        self.current_page = constants.DEFAULT_MIN_PAGE
        self.max_page = constants.DEFAULT_MAX_PAGE
        self.init_pages()

        self.result_imos = []

    def open_session(self):
        """Entry point of SearchSession."""
        yield self.session.start(callback=self.advanced_search)

    def advanced_search(self, response):
        """Callback function after SearchSession has opened.

        1. Go through search parameters list, do the search
        2. Scanning search result and aggregate vessel imos
        3. After all parameters list is done, parse all the vessels with imo list

        Args:
            response (scrapy.Response):

        Returns:

        """

        def _request_failed(error):
            if not self.session.should_retry_when_failed(error):
                # skip current page
                self.current_page += 1

            return self.advanced_search(response)

        if len(self.search_filters) > 0:
            search_form = api.search_form(**self.search_filters[0])

            # dont_filter flag is important because otherwise lots of request are dropped as
            # duplicates since duplicate does not know server state
            return FormRequest(
                url=api.SEARCH_URL,
                headers=api.SEARCH_HEADERS,
                formdata=search_form,
                meta=response.meta.copy(),
                dont_filter=True,
                callback=self.on_search_results,
                errback=_request_failed,
            )

        # aggregate results
        elif self.result_imos:
            logger.info('Searching vessels is done, moving to vessel parsing with imo.')
            return self.session.spider.parse_vessels(self.result_imos)

    @random_delay(average=constants.AVG_DELAY)
    def on_search_results(self, response):
        """Handle results with given search parameters, scan next page if any.

        Args:
            response (scrapy.Response):

        Returns:

        """
        if self.session.is_blocked(response):
            self.session.rotate(callback=self.advanced_search)

        if self.session.has_no_result(response):
            # retry, this is because of timeout
            yield self.advanced_search(response)

        # get imos from current page
        imos = api.get_imos(response)
        self.result_imos.extend(imos)

        scan_next_page = api.has_next_page(response) and self.current_page < self.max_page
        if scan_next_page:
            self.session.spider.crawler.stats.inc_value('equasis/pages_count')
            self.current_page += 1
            self.search_filters[0].update(min_page=self.current_page)

        else:
            logger.info(
                f'Search results sum-up: '
                f'found {len(imos)} on current page, '
                f'found {len(self.result_imos)} all together, '
                f'with search params: {self.search_filters[0]} '
                f'[session_id={self.session.id}]'
            )

            self.search_filters.pop(0)
            self.init_pages()

        # rotate and spread usage across all credentials to minimise chances of being banned
        self.session.stay_or_rotate(callback=self.advanced_search)

        # search for params list
        yield self.advanced_search(response)

    def init_pages(self):
        if len(self.search_filters) > 0:
            self.current_page = self.search_filters[0].get('min_page', constants.DEFAULT_MIN_PAGE)
            self.max_page = self.search_filters[0].get('max_page', constants.DEFAULT_MAX_PAGE)


class VesselSession(object):
    """VesselSession for Equasis website.

    VesselSession searches for vessels by its imo, and goes directly to vessel page to get
    detailed vessel information. It would only be called with given imo list, or after all
    the imos are aggregated in SearchSession.

    This session is able to:
        - redo searching by imo without changing credentials
        - rotate credentials after the search count has reached search quota
        - restart, skip current imo searching if failure

    """

    def __init__(self, spider, imos, whitelist=None, blacklist=None):
        self.session = EquasisSession(spider, self.__class__.__name__)
        self.imos = imos

        # allow blacklisting/whitelisting fields of the item
        # NOTE both whitelist/blacklist cannot be specified together
        if whitelist and blacklist:
            logger.error('Whitelist and blacklist cannot be specified simultaneously')
            raise CloseSpider('finished')

        self.whitelist = whitelist if whitelist else set()
        # only add mandatory field if whitelist specified to prevent activating this
        # even when we don't want a whitelist
        if self.whitelist:
            self.whitelist |= {'imo', 'provider_name'}

        self.blacklist = blacklist if blacklist else set()
        self.blacklist -= {'imo', 'provider_name'}  # mandatory fields

    def open_session(self):
        """Entry point of VesselSession."""
        return self.session.start(callback=self.request_vessel)

    def request_vessel(self, response):
        """Callback function after VesselSession opened.

        Args:
            response (scrapy.Response):

        Returns:

        """

        def _request_failed(error):
            if not self.session.should_retry_when_failed(error):
                # skip current imo
                self.imos.pop(0)

            return self.request_vessel(response)

        if len(self.imos) > 0:
            return api.make_vessel_request(
                imo=self.imos[0],
                response=response,
                callback=self.parse_vessel,
                errback=_request_failed,
            )

        else:
            logger.info(f'Parsed all imos [session_id={self.session.id}]')

    @random_delay(average=constants.AVG_DELAY)
    def parse_vessel(self, response):
        """Parse the vessel page with given imo.

        Args:
            response (scrapy.Response):

        Returns:
            Dict[str, str]:

        """
        if self.session.is_blocked(response):
            self.session.rotate(self.request_vessel)

        current_imo = self.imos[0]

        logger.debug(f'Parsing vessel imo={current_imo} [session_id={self.session.id}]')
        self.imos.pop(0)

        if self.session.has_no_result(response):
            logger.warning(
                f'Vessel not found with imo={current_imo} [session_id={self.session.id}]'
            )

        else:
            try:
                yield parser.parse_vessel_details(
                    selector=response,
                    provider=self.session.spider.provider,
                    whitelist=self.whitelist,
                    blacklist=self.blacklist,
                )

            except Exception as err:
                logger.warning(
                    f'Failed to parse item: '
                    f'response={response}, '
                    f'err={err} '
                    f'[session={self.session.id}]',
                    exc_info=1,
                )

        # will close the runtime if close to memory limit
        # after saving the given data dict
        self.session.spider.handle_high_memory({'imos_queue': self.imos})

        self.session.stay_or_rotate(callback=self.request_vessel)

        # parse next imo
        yield self.request_vessel(response)
