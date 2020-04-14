import json
import logging
from urllib.parse import urlencode

from scrapy import Request


logger = logging.getLogger(__name__)


class DubaiTradeSession:
    _vessel_lineup_url = 'http://dpwdt.dubaitrade.ae/pmisc/vesselinfo.do?{params}'
    _shipping_agent_url = 'http://dpwdt.dubaitrade.ae/pmisc/getRotnDtls.do?{params}'

    # cache max pagination returned by API
    max_page = 1

    def __init__(self, port, terminal, movement, callback):
        # these are mandatory fields by the API
        self.port = port
        self.terminal = terminal
        self.movement = movement

        # store spider callback so it can hook into the spider
        self.callback = callback

    def traverse_all(self):
        """Wrapper for obtaining the last page in the pagination list.

        This is not actually the traversal function for all pages, however it is named
        as such to provide an abstraction to get around scrapy's async workflow, as we
        need to first define the `max_page` attribute before we get all the pages. This
        cannot be done in `__init__` since scrapy requires that we return all requests
        to a callback.

        The ACTUAL traversal function is named below as `self._traverse_all`.

        """
        # first, we need to get the last page number of all the paginated tables
        return self.traverse(page=1, callback=self._init_max_page)

    def _traverse_all(self):
        """Traverse all paginated tables, given `port`, `terminal` and `movement`.

        This is the ACTUAL traversal function for all pages.
        The previous `self.traverse_all` function is merely an abstraction to get around
        scrapy's async workflow.

        """
        logger.debug('Extracting data from pages 1 -> %s', self.max_page)
        for page in range(1, self.max_page):
            yield from self.traverse(page=page, callback=self.callback)

    def traverse(self, page, callback):
        url = self._vessel_lineup_url.format(
            params=urlencode(
                {
                    'arrivedfrom': '',  # previous port of call
                    'd-2753038-p': page,  # page number
                    'days': 7,  # days ahead
                    'days1': 7,  # days before
                    'lookuptype': 'getVesselInformation',
                    'remCacheFlag': 'false',
                    'rotationNumber': '',  # internal portcall id
                    'port': self.port,
                    'sailto': '',  # next destination
                    'terminal': self.terminal,
                    'typeofInformation': self.movement,  # vessel movement type
                    'vesselName': '',
                }
            )
        )
        yield Request(url=url, callback=callback)

    def _init_max_page(self, response):
        # string returned should be of this format:
        # "Page <INT> of <INT>"
        pagination = response.xpath('//div[@id="pageDiv"]/font/text()').extract_first()
        if not pagination:
            raise ValueError('Unable to find max pagination')

        self.max_page = int(pagination.split()[-1])

        logger.debug(f'Max page for terminal "{self.terminal}": {self.max_page}')

        return self._traverse_all()

    @classmethod
    def get_shipping_agent(cls, rotation_id, callback=None):
        url = cls._shipping_agent_url.format(
            params=urlencode({'method': 'getRotnDtls', 'rotationNumber': rotation_id})
        )
        return Request(url=url, callback=callback)

    @classmethod
    def parse_shipping_agent(cls, response):
        res = json.loads(response.text).get('rows')
        if not res or len(res) == 0:
            logger.warning('Unable to extract shipping agent: %s', json.loads(response.text))
            return {}

        return res[0]
