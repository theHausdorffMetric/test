"""Hook Sentry to spider logs and exceptions.

Settings:

    - SENTRY_DSN:        as provided on Sentry web console
    - SENTRY_THRESHOLD:  minimum level to send log messages to Sentry

Context:

    - release: __version__
    - environment: SCRAPY_PROJECT_ID
    - server_name: scrapinghub | local


"""

from bdb import BdbQuit
import logging
import os
import platform
import socket
import sys

from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.exceptions import CloseSpider, NotConfigured
from sentry_sdk import configure_scope, init as sentry_init
from sentry_sdk.integrations.logging import LoggingIntegration

import kp_scrapers as application
from kp_scrapers.settings import is_shub_env


logger = logging.getLogger(__name__)


# sane default threshold for tracking log messages with Sentry
_DEFAULT_THRESHOLD = 'ERROR'


def _init_sentry(dsn, integrations):
    """Initialise Sentry error tracking.

    For details, see:
    https://docs.sentry.io/error-reporting/configuration/?platform=python

    Args:
        dsn (str): Sentry DSN (can be obtained from web console)
        integrations (List[sentry.integrations]): frameworks to hook into Sentry client

    """
    environment = os.getenv('SCRAPY_PROJECT_ID', 'local')  # dependant on `scrapy-jobparameters`
    server_name = 'scrapinghub' if is_shub_env() else 'local'

    sentry_init(
        dsn=dsn,
        integrations=integrations,
        before_send=_filter_exceptions,
        before_breadcrumb=_filter_breadcrumbs,
        in_app_include=[application.__package__],
        release=application.__version__,
        environment=environment,
        server_name=server_name,
        # request_bodies='always',
        # attach_stacktrace=True,  # send stacktraces together with log messages
    )


def _get_logging_conf(threshold):
    # by default, sentry already integrates python built-in `logging`
    # this just brings more explicit control over the configuration
    return LoggingIntegration(
        event_level=threshold,
        # hardcoded, since it doesn't bring value to vary breadcrumb level
        level=logging.INFO,
    )


def _filter_exceptions(event, hint):
    """Prevent redundant exceptions from being tracked.

    For details, see:
    https://docs.sentry.io/error-reporting/configuration/filtering/?platform=python#before-send

    """
    if 'exc_info' in hint:
        exc_type, exc_value, tb = hint['exc_info']
        if isinstance(exc_value, (CloseSpider, BdbQuit)):
            return None

    return event


def _filter_breadcrumbs(crumb, hint):
    """Prevent redundant breadcrumbs (i.e. logs) from being tracked.

    For details on a breadcrumb's data structure, see:
    https://docs.sentry.io/enriching-error-data/breadcrumbs/?platform=python

    """
    # NOTE this is here mainly as a demonstation and is practically redundant
    if crumb['category'] == 'a.spammy.Logger':
        return None

    return crumb


def log_level(name: str):
    """Convert human level name to expected program integer.

    Examples:
        >>> log_level('debug')
        10
        >>> log_level('dEbUg')
        10
        >>> log_level('CRITICAL')
        50
        >>> log_level('foo')

    """
    # yep, no public method...
    return logging._nameToLevel.get(name.upper())


def _os_version():
    # syntactic sugar for getting current OS version
    return platform.platform()


class SentryErrorTracker:
    def __init__(self, crawler: Crawler):
        self.dsn = crawler.settings.get('SENTRY_DSN')
        if not self.dsn:
            raise NotConfigured("SENTRY_DSN is not configured, disabling extension")

        self.threshold = crawler.settings.get('SENTRY_THRESHOLD', _DEFAULT_THRESHOLD)
        if not log_level(self.threshold):
            # this is a likely a human error it should be alerted, and should not
            # prevent production from running since we can fallback on a sane default
            logger.warning(
                'Invalid log level provided: %s (fallback to %s)',
                self.threshold,
                _DEFAULT_THRESHOLD,
            )
            self.threshold = _DEFAULT_THRESHOLD
        self.threshold = log_level(self.threshold)

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        return ext

    def spider_opened(self, spider):
        logging_integration = _get_logging_conf(threshold=self.threshold)
        _init_sentry(dsn=self.dsn, integrations=[logging_integration])

        # append global context to events that would be useful everywhere
        with configure_scope() as scope:
            # obtain user attributes
            _user = 'scrapinghub' if is_shub_env() else socket.gethostname().replace('.local', '')
            scope.user = {'username': _user}

            # cache cli for reproducibility
            scope.set_extra('cli', ' '.join(sys.argv))
            # more could be extracted from scrapinghub's env, see:
            # https://shub.readthedocs.io/en/stable/custom-images-contract.html#environment-variables
            scope.set_extra('job', os.getenv('SCRAPY_PROJECT_ID', 'unknown'))

            # index server setup for searchability
            scope.set_tag('spider', spider.name)
            scope.set_tag('system', _os_version())
