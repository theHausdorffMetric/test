import logging
import random

from scrapy import Selector
from scrapy.http import Request

from kp_scrapers.settings.network import USER_AGENT_LIST
import kp_scrapers.spiders.registries.equasis.parser as parser


logger = logging.getLogger(__name__)

# TODO enum ?
DROPDOWN_MODE_CODES = {'all': 'TT', 'ignore': 'HC', 'chose': 'CM', 'exclude_all': 'AU'}

INCLUDE_HISTORY = 'on'

SEARCH_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip,deflate',
    'Accept-Language': 'en-GB,en;q=0.5',
    'Cache-Control': 'no-cache',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Host': 'www.equasis.org',
    'Pragma': 'no-cache',
    'Referer': 'http://www.equasis.org/EquasisWeb/public/HomePage?fs=HomePage',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': random.choice(USER_AGENT_LIST),
}

SHIP_URL = 'http://www.equasis.org/EquasisWeb/restricted/ShipInfo?fs=Search'
SEARCH_URL = 'http://www.equasis.org/EquasisWeb/restricted/Search?fs=Search'
SEARCH_PARAMS = {
    'P_PAGE': '1',
    'P_PAGE_COMP': '1',
    'P_PAGE_SHIP': '1',
    'ongletActifSC': 'ship',
    'P_ENTREE_HOME_HIDDEN': '',
    'P_IMO': '',
    'P_CALLSIGN': '',
    'P_NAME': '',
    'P_NAME_cu': INCLUDE_HISTORY,
    'P_MMSI': '',
    'P_GT_GT': '',
    'P_GT_LT': '',
    'P_DW_GT': '',
    'P_DW_LT': '',
    'P_YB_GT': '',
    'P_YB_LT': '',
    'P_CLASS_rb': DROPDOWN_MODE_CODES['ignore'],
    'P_CLASS_ST_rb': DROPDOWN_MODE_CODES['ignore'],
    'P_FLAG_rb': DROPDOWN_MODE_CODES['ignore'],
    'P_CatTypeShip_rb': DROPDOWN_MODE_CODES['ignore'],
    'buttonAdvancedSearch': 'advancedOk',
}

PARAMS_EQUASIS_MAPPING = {
    'min_page': 'P_PAGE_SHIP',
    'imo': 'P_IMO',
    'call_sign': 'P_CALLSIGN',
    'vessel_name': 'P_NAME',
    'mmsi': 'P_MMSI',
    'min_gt': 'P_GT_GT',
    'max_gt': 'P_GT_LT',
    'min_dwt': 'P_DW_GT',
    'max_dwt': 'P_DW_LT',
    'min_build_year': 'P_YB_GT',
    'max_build_year': 'P_YB_LT',
}


def search_form(**kwargs):
    # advanced search form
    # form request doesn't understand int so we cast to str
    formdata = SEARCH_PARAMS
    ship_category = kwargs.get('ship_category', '')
    if ship_category:
        formdata.update(
            {
                'P_CatTypeShip_rb': DROPDOWN_MODE_CODES['chose'],
                'P_CatTypeShip': ship_category,
                'P_CatTypeShip_p2': ship_category,
            }
        )

    formdata.update(
        {
            'P_NAME': kwargs.get('vessel_name', ''),
            'P_PAGE_SHIP': str(kwargs.get('min_page', '1')),
            'P_YB_GT': str(kwargs.get('min_build_year', '')),
            'P_YB_LT': str(kwargs.get('max_build_year', '')),
            'P_DW_GT': str(kwargs.get('min_dwt', '')),
            'P_DW_LT': str(kwargs.get('max_dwt', '')),
        }
    )
    formdata.update(kwargs.get('extra_filters', {}))
    return formdata


def make_vessel_request(imo, response, callback, errback):
    return Request(
        '{base}&P_IMO={imo}'.format(base=SHIP_URL, imo=imo),
        headers=SEARCH_HEADERS,
        callback=callback,
        errback=errback,
        dont_filter=True,
        meta=response.meta.copy(),
    )


def get_imos(response):
    selector = Selector(response)
    return parser.parse_imos_from_search_results(selector)


def has_no_results(html):
    # check that will work respectively for vessel search and for advanced search results
    # NOTE if argument is passed as a `scrapy.response.body`, it will be encoded as a bytestring
    return 'No ship has been found' in str(
        html
    ) or 'No company nor Ship has been found with your criteria !' in str(html)


def is_blocked(response):
    """Check if we have been logged out or blocked from the website.

    Note that if no vessel is found, the same warning modal will also display, but with a
    different message. In this case, we still mark it as success since we are technically
    not blocked, just that we find no vessel.

    Args:
        response (scrapy.Response):

    Returns:
        Tuple(bool, str): tuple of (is_blocked, response_message)

    """
    login_res = response.xpath('//div[@id="warning"]//p/text()').extract_first()
    if login_res is None or 'has been found with your criteria' in login_res:
        return False, 'success'
    else:
        return True, login_res


def has_next_page(response, next_symbol='>'):
    """Check if there are more pages after the current one.

    We assume that if we can't find > on the paginatino row, then we reach the
    end of available restult pages.

    """
    # extract paging text and strip out all the fluffy '\n' and the like
    pages = [v.strip() for v in parser.parse_page_links(Selector(response))]
    # if falsy, return False immediately
    if not pages:
        return

    # no need to crash four counter debugging, but this is unlikely to happen
    try:
        total, current_last = parser.parse_number_of_results(Selector(response))
        logger.debug('currently working on results {}/{}'.format(current_last, total))

        if next_symbol not in pages and current_last != total:
            # should be impossible
            raise RuntimeError('no next page but we didnt reach all the results')
    except ValueError as e:
        logger.warning('unable to parse result counters: {}'.format(e))

    # search for the $next_symbol, meaning there is still pages left
    return next_symbol in pages
