import logging
from urllib.parse import urlencode

from scrapy import Request


logger = logging.getLogger(__name__)


URL_LOGIN = 'https://app.importgenius.com/users/auth'
URL_SEARCH = 'https://app.importgenius.com/main/maketable2'
URL_MAPPING = 'https://app.importgenius.com/main/grid/im'
URL_SHOW_EXPORT_COUNT = 'https://app.importgenius.com/export/show_export_counts'
URL_GENERATE_CSV = 'https://app.importgenius.com/export/generate'
URL_DOWNLOAD_CSV_BASE = 'https://app.importgenius.com/export/download/{sid}/{pid}'
URL_STATUS = 'https://app.importgenius.com/export/status2/{pid}'
URL_VIEWED = 'https://app.importgenius.com/export/viewed/{pid}'
URL_PREP = 'https://app.importgenius.com/export/prep_for_download/{pid}'

HEADERS = [
    ('Origin', 'https://app.importgenius.com'),
    ('Accept-Encoding', 'gzip, deflate, br'),
    ('Accept-Language', 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7'),
    ('Content-Type', 'application/x-www-form-urlencoded'),
    ('Accept', 'application/json, text/javascript, */*; q=0.01'),
    ('Referer', 'https://app.importgenius.com/'),
    ('X-Requested-With', 'XMLHttpRequest'),
    ('Connection', 'keep-alive'),
]

CSV_EXPORT_FIELD = [
    'number',
    'product',
    'consname',
    'shipname',
    'actdate',
    'weightLB',
    'weightKG',
    'fport',
    'uport',
    'vesselname',
    'countryoforigin',
    'marks',
    'consaddr',
    'shipaddr',
    'zipcode',
    'con_num_count',
    'containernum',
    'conttype',
    'manifestqty',
    'manifestunit',
    'measurement',
    'measurementunit',
    'billofladingnbr',
    'masterbillofladingind',
    'portname',
    'masterbilloflading',
    'voyagenumber',
    'seal',
    'countryname',
    'inboundentrytype',
    'carriercode',
    'carrier_name',
    'city',
    'state',
    'czipcode',
    'address',
    'ntfyname',
    'ntfyaddr',
    'placereceipt',
]

COMMON_QUERY_PARAMETERS_BY_CRITERIA = [
    ('mtype[]', 'ALL'),
    ('qzip[]', ''),
    ('qmiles[]', 1),
    ('qzip1[]', ''),
    ('qzip2[]', ''),
    ('qkeyname[]', ''),
]

COMMON_QUERY_PARAMETERS = [
    ('country', 'us'),
    ('datatype', 'im'),
    ('datatype_map', 'im'),
    ('sortname', 'actdate'),
    ('sortorder', 'desc'),
    ('ctype', 'consname'),
    ('cname', ''),
]


def login(username, password, callback):
    return Request(
        url=URL_LOGIN,
        method='POST',
        headers=HEADERS,
        body=_auth_form(username, password),
        callback=callback,
    )


def get_mapping(callback):
    return Request(
        url=URL_MAPPING, method='GET', headers=HEADERS, callback=callback, errback=_request_failed
    )


# TODO factorise queries in main `spider.py`
def query():
    pass


def build_query(query, filters):
    """Build search query parameters.

    Args:
        query (str): actual serach query
        filters (list(tuple(str, str))): options to restrict range of query results obtained

    Returns:
        str: urlencoded query string

    """
    parameters = list(filters)
    parameters.extend(COMMON_QUERY_PARAMETERS)

    if query.split(' ')[0].strip() != 'AND':
        query = 'AND ' + query

    for w in query.split(','):
        w = w.strip()
        head = w.split(' ')[0]
        tail = ' '.join(w.split(' ')[1:])

        assert head in ('AND', 'NOT', 'OR')

        parameters.append(('cond[]', head))
        parameters.append(('qtype[]', 'product'))
        parameters.append(('qry[]', tail))
        parameters.extend(COMMON_QUERY_PARAMETERS_BY_CRITERIA)

    return urlencode(parameters)


def build_not_terms(query):
    """Build not terms list from query

    Args:
        query (str): input query

    Returns:
        list: list of 'not' terms

    Examples:
        >>> build_not_terms('OR ulsd, OR unleaded, NOT alcohol*, NOT coconut')
        ['alcohol', 'coconut']
    """
    return [
        term.replace('not', '').replace('*', '').strip()
        for term in query.replace('\n', '').lower().split(',')
        if 'not' in term
    ]


def _auth_form(username, password):
    return urlencode([('action', 'users/auth'), ('nusername', username), ('password', password)])


def _request_failed(self, err):
    logger.error('Unable to login to ImportGenius: %s', err)
    return None
