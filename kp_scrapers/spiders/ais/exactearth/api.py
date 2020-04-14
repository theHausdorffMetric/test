"""Exact Earth API helpers.
"""

from __future__ import absolute_import

from . import constants


def wrap_xml_filters(filters):
    return '<Filter><And>{}</And></Filter>'.format(''.join(filters))


def ee_request_url(token, filters, version='1.1.0'):
    SERVICE = 'WFS'
    # request vessel information and position
    REQUEST = 'GetFeature'
    # only fetch last update
    TYPE = 'exactAIS:LVI'

    # build a templat of the enpoint
    SERVICE_URL_TPL = ''.join(
        [
            '{base}',
            '?service={service}',
            '&version={version}',
            '&request={request}',
            '&typeName={type_name}',
            '&authKey={token}',
            '&{filters}',
        ]
    )

    params = {
        'base': constants.SERVICE_BASE_URL,
        'token': token,
        'version': version,
        'service': SERVICE,
        'request': REQUEST,
        'type_name': TYPE,
    }
    params.update({'filters': filters})

    # NOTE urlencode ?
    return SERVICE_URL_TPL.format(**params)


def serialize_date(py_date):
    """
    Examples:
        >>> serialize_date('2017-11-07 11:00:45.119264')
        '20171107110045119264'

    """
    # NOTE or strftime('%Y%m%d%H%M%S') (works with Python 3)
    bad_chars = ' -:.'
    translator = str.maketrans('', '', bad_chars)
    return str(py_date).translate(translator)


def wrap_cql_filters(*filters):
    """
    Examples:
        >>> wrap_cql_filters('foo', 'bar')
        'CQL_FILTER=foo AND bar'
        >>> wrap_cql_filters('foo')
        'CQL_FILTER=foo'

    """
    # NOTE should we urlencode here ?
    return 'CQL_FILTER={}'.format(' AND '.join(filters))


class ECQLRequestFactory(object):
    def __init__(self, token):
        self.token = token
        self.filters = []

    def since(self, since_date):
        # NOTE for unknown reasons, api returns no results if "exclusive >" is used,
        #      despite it being a valid ECQL operator
        cond = "ts_insert_utc >= '{}'".format(serialize_date(since_date))
        self.filters.append(cond)
        return self

    def match(self, key, values):
        cond = "{} in ('{}')".format(key, "','".join(values))
        self.filters.append(cond)
        return self

    def build(self):
        cql_filters = wrap_cql_filters(*self.filters)
        return ee_request_url(self.token, cql_filters)


class XMLRequestFactory(object):
    def __init__(self, token):
        self.token = token
        self.filters = []

    def match(self, name, value):
        self.filters.append(constants.FILTER_EQUAL.format(name=name, value=value))

    def diff(self, name, hours):
        self.filters.append(constants.FILTER_WINDOW.format(name=name, hours=hours))

    def range(self, name, lower, upper):
        self.filters.append(constants.FILTER_BETWEEN.format(name=name, lower=lower, upper=upper))

    def exclude(self, filter_func, *args, **kwargs):
        # add the requested filter to the list
        filter_func(*args, **kwargs)
        # wrap it with the negation
        self.filters[-1] = '<Not>{}</Not>'.format(self.filters[-1])

    def generate_url(self):
        filters = 'filter={}'.format(wrap_xml_filters(self.filters))
        return ee_request_url(self.token, filters)
