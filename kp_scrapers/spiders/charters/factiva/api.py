import json
from urllib.parse import unquote, urlencode

from kp_scrapers.lib.parser import may_strip


_XFORMSESSSTATE = (
    'UnswOnsyOnswOjAsMjoiTlMiLDM6W10sNDoiZW4iLDU6MCw'
    '5OiJHTCJ9LFU6Ii9TZWFyY2gvU1NSZXN1bHRzIn19EQAAQQAAAA=='
)

SEARCH_MODEL = {
    "pid": "",
    "pname": "",
    "ptype": "",
    "searchFilter": {
        "ft": "Dirty Tanker Fixtures",
        "relCont": 'true',
        "sl": {"slid": "All", "slds": "false", "sljson": "[]", "sln": "All Sources"},
        "hch": "All",
        "dt": "Last3Months",
        "so": "Relevance",
        "frtr": '0',
        "mrtr": '20',
        "ddm": "Similar",
        "snt": "Fixed",
        "mt": "Highlight",
        "df": "MMDDCCYY",
        "sa": '0',
        "ism": 'false',
        "sfc": {"code": "", "desc": "Dirty Tanker Fixtures"},
    },
}


def oauth_form(url):
    _ref = _parse_query_string_params(url)
    username = {
        'user_id': 'ylchay@kpler.com',
        'state': _ref.get('login_hint'),
        'lang': 'en',
        'kmli': '0',
        'productname': 'global',
        'nsConflict': 'Y',
    }
    return {
        'scope': _ref.get('scope'),
        'response_type': _ref.get('response_type'),
        'username': json.dumps(username),
        'password': 'kplerkpler',
        'connection': _ref.get('connection'),
        'state': _ref.get('state'),
        'callbackURL': _ref.get('redirect_uri'),
        'sso': 'true',
        'client_id': _ref.get('client'),
        'redirect_uri': _ref.get('redirect_uri'),
        'tenant': 'auth',
    }


def login_form(url):
    _ref = _parse_query_string_params(url)
    return {'state': _ref.get('state')}


def login_query_string(url):
    _ref = _parse_query_string_params(url)
    return urlencode({'emgls_auth_code': _ref.get('code'), 'productname': 'global'})


def search_form(date):
    if date:
        SEARCH_MODEL['searchFilter']['ft'] = 'Dirty Tanker Fixtures' + ' ' + date
    return {
        '_XFORMSESSSTATE': _XFORMSESSSTATE,
        '_XFORMSTATE': '',
        'ins': '1',
        'napc': 'NS',
        'searchModel': json.dumps(SEARCH_MODEL),
    }


def article_form(result_selector):
    data_cqs = result_selector.xpath('./div[@id="ssResultsHeadlinesPHL"]/@data-cqs').extract_first()
    article_meta = _extract_article_meta(result_selector.xpath('./script').extract_first())
    if not article_meta:
        return

    article_request = {
        'an': article_meta.get('reference').get('guid'),
        'format': 'FULL',
        'cqs': data_cqs,
        'docVector': article_meta.get('documentVector'),
        'ref': '',
        'mimeTYype': 'text/xml',
        'contCat': article_meta.get('contentCategoryDescriptor'),
    }

    return {
        'articleRequest': json.dumps(article_request),
        '_XFORMSESSSTATE': _XFORMSESSSTATE,
        '_XFORMSTATE': '',
    }


def _parse_query_string_params(url):
    """Parse query string params from url.

    Args:
        url (str):

    Returns:
        Dict[str, str]:

    """
    res = {}
    params = url.split('?')[1].split('&')

    for param in params:
        key, _, value = param.partition('=')
        res.update({key: unquote(value)})
    return res


def _extract_article_meta(script):
    """Extract article meta data from js script.

    The snippet is like, the info assigned to SSResults.ssHeadlinesPhlResult is what we needed.
    <script type="text/javascript">
        if (typeof (SSResults) == 'undefined')
            SSResults = {};

        SSResults.ssHeadlinesPhlResult = {......}
        SSResults.pictureTNails = {"result":null,"error":null};
        SSResults.multimediaTNails = {"result":null,"error":null};
        SSResults.MaxHeadlinesPostProcessingLimit = 100;
        SSResults.IsAcademicUser = false;
        SSResults.HasAuthorListsAccess = true;
        SSResults.IsAuthorListEnabled = true;

        var showAccessionNumber = false;

    </script>

    Args:
        script (str):

    Returns:
        Dict[str, str]:

    """
    if not script:
        return

    raw_meta = script.split('SSResults')[3]
    start_idx, end_idx = raw_meta.find('{'), raw_meta.rfind('}')
    _wrapped = json.loads(may_strip(raw_meta[start_idx : end_idx + 1]))

    for article in _wrapped.get('result').get('resultSet').get('headlines'):
        if 'Dirty Tanker Fixtures' in article.get('title'):
            return article
