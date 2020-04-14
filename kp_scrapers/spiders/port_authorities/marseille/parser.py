from scrapy import Selector

from kp_scrapers.lib.utils import map_keys


def html_response(response):
    """Forcibly convert HTML-like response to a proper HtmlResponse.

    Scrapy may incorrectly autoselect wrong response type,
    depending on response body semantics with custom frameworks (e.g. JavaServer Faces).

    Args:
        response (scrapy.Response):

    Returns:
        scrapy.HtmlResponse:

    """
    return Selector(text=response.text, type='html')


class PortCallJsfForm:
    """Pretty API around a JSF form for querying portcalls.

    To initialise, `source` and `viewstate` are required.
    `source` describes the search classification/category to be performed.
    Unlike its ASP.NET counterpart, `viewstate` behaves more like a cookie
    and should not need to be updated per request.

    """

    # NOTE allow mapping of form fields into human queries, for readability
    # TODO not exhaustively mapped; source supports more query filters but these should suffice
    _QUERY_MAPPING = {
        'arrival_end': ('j_idt2:form-pitalugue:j_idt66:valueTo', None),
        'arrival_start': ('j_idt2:form-pitalugue:j_idt66:valueFrom', None),
        'departure_end': ('j_idt2:form-pitalugue:j_idt66:valueTo', None),
        'departure_start': ('j_idt2:form-pitalugue:j_idt66:valueFrom', None),
        'next_port': ('j_idt2:form-pitalugue:j_idt92', None),
        'previous_port': ('j_idt2:form-pitalugue:j_idt91', None),
        'status': ('j_idt2:form-pitalugue:j_idt55:input', None),
        'vessel_type': ('j_idt2:form-pitalugue:j_idt85:input', None),
    }

    def __init__(self, source, viewstate):
        self._viewstate = viewstate
        self._form = {
            'j_idt2:form-pitalugue': 'j_idt2:form-pitalugue',
            # allow partial forms
            'javax.faces.partial.ajax': 'true',
            # source strings
            'javax.faces.partial.execute': '@all',
            'javax.faces.partial.render': 'j_idt2:form-pitalugue',
            'javax.faces.source': source,
            source: source,
            # view state (similar to ASP.NET viewstate)
            'javax.faces.ViewState': viewstate,
        }

        # special field values required if requesting vessel detail
        if self._is_request_vessel(source):
            self._form['javax.faces.partial.execute'] = source
            self._form['javax.faces.partial.render'] += ':escale'

    @property
    def viewstate(self):
        return self._viewstate

    def asdict(self):
        # standardised interface for obtaining instance as dict
        return self._form

    def query(self, **options):
        """Fluent method for adding query options to JSF form.

        Allows punch-through update of JSF form fields, if required.

        """
        # `skip_missing = False` required for punch-through capability
        self._form.update(map_keys(options, self._QUERY_MAPPING, skip_missing=False))

    @staticmethod
    def _is_request_vessel(source):
        """Check if form is for requesting portcalls, or for vessel attributes.
        """
        return 'escale' in source
