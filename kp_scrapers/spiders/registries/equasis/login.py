import datetime as dt
import logging

from scrapy.exceptions import CloseSpider
from scrapy.http import FormRequest

import kp_scrapers.spiders.registries.equasis.api as api
import kp_scrapers.spiders.registries.equasis.constants as constants
import kp_scrapers.spiders.registries.equasis.utils as utils


logger = logging.getLogger(__name__)


def old_enough(login, tolerance=constants.BANNED_TIME):
    """Check if the provided login's last ban is older than `tolerance`."""
    utc_now = dt.datetime.utcnow()
    from_now = utils.to_timestamp(utc_now) - (login.get('_last_blocked') or 0)
    return from_now > tolerance


def oldest_login(logins):
    """Select login that was used the least recently.

    Note that logins with no last used value are implied to have never been used before.
    Therefore, they are prioritised when it comes to selecting the "oldest" login.

    Args:
        logins (List[Dict(str, str)]): list of available credentials

    Returns:
        Dict[str, str]: chosen login details with the oldest `last_used`

    Examples:  # noqa
        >>> oldest_login([{'last_used': '2018-03-30'}, {'last_used': '2018-08-30'}, {'last_used': '2017-12-30'}])
        {'last_used': '2017-12-30'}
        >>> oldest_login([{'last_used': '2018-03-30'}, {'last_used': '2018-08-30'}, {'last_used': None}])
        {'last_used': None}

    """
    return min(logins, key=lambda login: login['last_used'] or '0')


def rebuild_logins(persisted_creds, creds):
    """Initialise a dict of all logins for Equasis, with associated meta info.

    This function will append new logins if there is a mismatch of logins between
    the persistent storage and the `constants` login list, and keep the old logins if they exist.

    Args:
        persisted_creds (Dict[str, Dict[str, str]]): dict of credentials with {login: {login_meta}}
        creds (List[Dict[str, str]]): list of credentials (login, password, created_at)

    Returns:
        Dict[str, Dict[str, str]]: dict of all available logins as {login: {login_meta}}

    """
    for cr in creds:
        # do not reinitialise login if it already exists
        if cr['login'] in persisted_creds:
            continue

        persisted_creds[cr['login']] = {
            'login': cr['login'],
            'password': cr['password'],
            # TODO should be filled in programatically by login creation script ?
            'created_at': cr.get('created_at'),
            # number of successful logins made in total
            'successful_logins': 0,
            # number of failed and successful logins made in total
            'total_logins': 0,
            # account metrics (to see if we are possibly banned forever)
            'last_used': None,
            'last_success': None,
            'last_response': '',
            # `last_blocked` used only programatically for checking if ban expired
            '_last_blocked': 0,
        }

    return persisted_creds


# NOTE this class could be much more generic
class Credentials(object):
    """Manage how to make and keep a scrapper logged in.

    Args:
        storage(PersistDataManager): make data persistent between runs
        credentials(list[dict]): list of login/password dictionnaries
    """

    def __init__(self, persisted_data, inventory, stats):
        self._credentials = {}
        self.inventory = inventory
        # keep scrapy `PersistSpider` convention
        self.persisted_data = persisted_data
        # closely monitor credntials lifecycle
        self.stats = stats

        # check if list of persisted logins is equal to the one we have in `constants`
        # if not, append new logins and keep old logins as they are
        if len(self.persisted_data.get('logins', {})) != len(inventory):
            # append new logins
            self.persisted_data['logins'] = rebuild_logins(
                self.persisted_data.get('logins', {}), inventory
            )

    def __getitem__(self, key):
        """Transparent access to expected credentials property."""
        return self._credentials[key]

    @property
    def available_logins(self):
        """Remove blocked logins that are still in the ban period."""
        return [login for login in self.persisted_data['logins'].values() if old_enough(login)]

    def renew(self):
        # filter to elligible logins
        logins = self.available_logins
        self.stats.set_value('equasis/logins', len(logins))
        if not logins:
            raise CloseSpider(reason='no logins left to use')

        logger.info('renewing credentials (%d left)', len(logins))
        # select login that was used the least recently
        self._credentials = oldest_login(logins)

    def success(self, login_res):
        self._update_meta('last_response', login_res)
        self._update_time('last_success')
        self._update_count('successful_logins')

    def use(self, login_res):
        self._update_meta('last_response', login_res)
        self._update_time('last_used')
        self._update_count('total_logins')

    def ban(self):
        self._update_meta('_last_blocked', utils.to_timestamp(dt.datetime.utcnow()))

    def _update_time(self, key):
        # store time as ISO8601 to be human-readable and compatible
        # remove milliseconds for readability
        self._update_meta(key, dt.datetime.utcnow().isoformat()[:-7])

    def _update_count(self, key):
        count = self.persisted_data['logins'][self._credentials['login']][key]
        self._update_meta(key, count + 1)

    def _update_meta(self, key, value):
        login = self._credentials['login']
        self.persisted_data['logins'][login][key] = value

        # save to update file in case other job are launched before this instance finished
        self.persisted_data.save()


class LoginPage(object):
    LOGIN_URL = 'http://www.equasis.org/EquasisWeb/authen/HomePage?fs=HomePage'
    REQUEST_PRIORITY = 100
    # prevent Scrapy scheduler from filtering identical requests
    ALLOW_IDENTICAL_REQUESTS = True
    REQUEST_HEADER = api.SEARCH_HEADERS

    def __init__(self, session, credentials, on_login):
        self.session = session
        self.credentials = credentials
        self.on_login = on_login

    @property
    def _form_meta(self):
        """Parameters to support multi-session per spider"""
        return {
            'session_name': self.session,
            # let one use several cookies per spider
            'cookiejar': self.session,
            # download_slot setting allows to use CONCURRENT_REQUEST and
            # DOWNLOAD_DELAY per slot
            # http://stackoverflow.com/questions/30970934/scrapy-different-download-delay-for-different-domain
            # https://github.com/scrapy/scrapy/blob/master/scrapy/extensions/throttle.py
            'download_slot': self.session,
        }

    @property
    def _form_data(self):
        return {
            'submit': 'Login',
            'j_email': self.credentials['login'],
            'j_password': self.credentials['password'],
        }

    def submit(self):
        self.credentials.renew()
        logger.debug('Submit new Login request with login {}'.format(self.credentials['login']))
        return FormRequest(
            url=self.LOGIN_URL,
            formdata=self._form_data,
            callback=self._on_login_attempt,
            errback=self._request_failed,
            headers=self.REQUEST_HEADER,
            priority=self.REQUEST_PRIORITY,
            dont_filter=self.ALLOW_IDENTICAL_REQUESTS,
            meta=self._form_meta,
        )

    def _request_failed(self, error):
        logger.warning('login request failed, retrying (err={})'.format(error))
        return self.submit()

    def _on_login_attempt(self, response):
        """Try another login if we were blocked."""
        is_blocked, login_res = api.is_blocked(response)
        self.credentials.use(login_res)
        if is_blocked:
            logger.warning(f'unable to login: {login_res}')
            self.credentials.ban()
            # try another login (the previous ones will be excluded from this
            # election since it was just used) with same metas
            return self.submit()
        else:
            logger.debug('logged in as {}'.format(self.credentials['login']))
            self.credentials.success(login_res)
            # A good login found, continue normal processing
            return self.on_login(response)
