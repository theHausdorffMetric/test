# -*- coding: utf-8; -*-

"""A set of :class:`scapy.http.Response` subclass that mocks the regular
subclasses used within the framework.

"""

from __future__ import absolute_import, unicode_literals
import functools
import os

from scrapy import Selector
from scrapy.http import HtmlResponse, Request, TextResponse, XmlResponse

from kp_scrapers.lib import utils


# This is intended as a mock for `static_data.vessesl()`.
VESSEL_LIST = (
    {'imo': '1234567', 'mmsi': '123456789', 'name': 'Vaiselle', 'providers': ['SS']},
    {'imo': '2345678', 'mmsi': '234567890', 'name': 'Vessel', 'providers': ['VT']},
    {'imo': '3456789', 'mmsi': '345678901', 'name': 'Boat', 'providers': ['MT_API']},
    {'imo': '4567890', 'mmsi': '456789012', 'name': 'Ship', 'providers': ['VF']},
    {'imo': '4567890', 'mmsi': '456789012', 'name': 'Navire', 'providers': ['SF']},
    {
        '_env': ['lpg_staging'],
        'name': 'moyra',
        'providers': ['EE', 'VF API', 'MT_API'],
        'mmsi': None,
        'imo': '9712553',
        'status_detail': 'Launched',
        '_markets': ['lpg'],
        'call_sign': None,
    },
)


class ForcedVesselList(list):
    """Fake list of vessels with the same interface."""

    def __init__(self, *args, **kwargs):
        # just let any instanciation to work
        super(ForcedVesselList, self).__init__()

    def load_and_cache(self, *args, **kwargs):
        return VESSEL_LIST

    def get(self, value, key='imo'):
        return utils.search_list(self, key, value)


class MockStaticData(object):
    @staticmethod
    def vessels(*args, **kwargs):
        return ForcedVesselList().load_and_cache()


class ForcedCollection(object):
    """Overwrite with pre-defined values SHUB collection content.

    We need to instanciate an object with a given list (i.e. the actual
    vessels mlock) that behaves like a function. Then once called without
    argument, it should return this initial list.

    Example:

        .. code-block::

            import tests._helpers.mocks as kp_mocks
            from mock import patch

            patch('path.to.tested.static_data.vessels', new=kp_mocks.Collection([vessel1, vessel2]))
            def some_unit_test(self):
                # static_data.vessels has now the value given to `new`
                pass

    """

    def __init__(self, fakes=None):
        self.fakes = fakes or []

    def __call__(self, *args, **kwargs):
        return self.fakes


class FakeRequestMixin(object):
    def __init__(self, *args, **kwargs):
        '''Fake response mixin that overrides a scrapy Request's constructor.

        TODO
        ----

            Intercept and override what needs to be overriden
        '''
        super(FakeRequestMixin, self).__init__(*args, **kwargs)


class FakeRequest(FakeRequestMixin, Request):
    pass


class FakeResponseMixin(object):
    def __init__(self, file_, *args, **kwargs):
        '''Fake response mixin that overrides a scrapy Response class
        constructor to read its body from a file.

        Parameters
        ----------

        '''

        with open(file_, 'r') as f:
            kwargs['body'] = f.read()

        url = 'file://{}'.format(os.path.abspath(file_))
        super(FakeResponseMixin, self).__init__(url=url, encoding='utf-8', **kwargs)


class FakeResponse(FakeResponseMixin, HtmlResponse):
    '''Fake response class to use within test cases.

    For improved compatibility, in case tests with isinstance or issubclass
    are used, we use a mixin to override the few methods we need. Every thing
    else is inherited from the real HtmlResponse class.

    TODO
    ----

       Some response handlers look at attributes that may not be well handled
       here. From the top of my head: ``response.meta``, ``response.request``.

    '''

    pass


class FakeTextResponse(FakeResponseMixin, TextResponse):
    '''Fake TextResponse class to use in test cases.

    Works exactly like FakeResponse, but is derived from TextResponse, the
    type :class:`scrapy.http.Response` Scrapy instanciate when the content-
    type of the received HTTP response is JSON, maybe others ...
    '''

    pass


class FakeXmlResponse(FakeResponseMixin, XmlResponse):
    '''Fake TextResponse class to use in test cases.

    Works exactly like FakeResponse, but is derived from TextResponse, the
    type :class:`scrapy.http.Response` Scrapy instanciate when the content-
    type of the received HTTP response is JSON, maybe others ...
    '''

    pass


class EmptyResponse(HtmlResponse):
    def __init__(self, *args, **kwargs):
        kwargs.update({'body': ''.encode('utf-8'), 'status': 200, 'encoding': 'utf-8'})

        super(EmptyResponse, self).__init__('http://www.kpler.net/', *args, **kwargs)


def response_factory(file_, klass=FakeResponse, meta=None, status=200):
    url = 'file://{}'.format(os.path.abspath(file_))
    request = FakeRequest(url=url, meta=meta)
    return klass(file_, request=request, status=status)


def fixtures_path(*args):
    # whatever the working directory, get module path
    module_path = os.path.dirname(os.path.dirname(__file__))

    # from there build the fixtures path
    return os.path.join(os.path.abspath(module_path), '_fixtures', *args)


def _selector_from_file(path):
    """Create a new scrapy selector from a file which path is provided as argument.
    """
    with open(fixtures_path(path)) as f:
        return Selector(text=f.read())


def selector_source(path):
    """Given a relative path to an html file, we build a scrapy Selector and pass
    it to the decorated function to use for testing parsing methods. This
    allows us to reproduce parsing errors, unit test them, and ensure they
    never come back to bite us because of a parsing refactor.
    """

    def test_decorator(func):
        def test_decorated(*args, **kwargs):
            args_ = list(args) + [_selector_from_file(path)]
            func(*args_, **kwargs)

        return test_decorated

    return test_decorator


def inject_fixture(path, loader=None):
    def _decorator(func):
        @functools.wraps(func)
        def _inner(klass):
            fixture = fixtures_path(path)

            if loader:
                return func(klass, loader(fixture))

            # default just read plain text
            with open(fixture, 'r') as f:
                return func(klass, f.read())

        return _inner

    return _decorator
