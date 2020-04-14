import logging
import re

from scrapy.http import FormRequest

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.spiders.port_authorities.colombia.utils import get_md5, RETRY_TIMES


logger = logging.getLogger(__name__)


class ColombiaSession:
    # last page that data is present on
    max_page = None

    # number of retries allowed before stopping spider
    # required because server will return internal errors occasionally for unknown reasons
    req_retries = RETRY_TIMES

    # this session's aspx states
    _viewstate = None
    _eventvalidation = None

    def __init__(self, homepage, start_date, end_date, on_traversal=None):
        self._response = homepage
        self.start_date = start_date
        self.end_date = end_date
        self.on_traversal = on_traversal if on_traversal else lambda x: x

        # init aspx states
        self._update_aspx_states(homepage)

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
        return self.traverse(page=1, callback=self.init_max_page)

    def init_max_page(self, response):
        """Initialise the last page number of all the paginated tables.

        Args:
            response (scrapy.Response):
        """
        # check for errors and retry, if any
        if not self._is_valid_response(response):
            return self.traverse(page=response.meta['page'], callback=self.init_max_page)

        # extract last page from pagination row
        row = response.xpath('//table[@id="ctl00_ContentPlaceHolder1_dgdReporteFletamento"]//tr')[
            -1
        ]

        pagination = [may_strip(x) for x in row.xpath('.//text()').extract() if may_strip(x)]

        # there is only one page and there is no pagination list
        if not pagination:
            self.max_page = 1
            return self._traverse_all()

        # there are more pages to check than is listed
        # e.g. ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '...']
        elif pagination[-1] == '...':
            # store updated aspx states first
            self._update_aspx_states(response)
            return self.traverse(page=int(pagination[-2]) + 1, callback=self.init_max_page)

        # there are a limited number of pages to check, and the last page is clearly stated
        # e.g. ['1', '2', '3', '4', '5', '6', '7']
        else:
            self.max_page = int(pagination[-1])
            return self._traverse_all()

    def _traverse_all(self):
        """Traverse all paginated tables, given `start_date` and `end_date` attributes.

        This is the ACTUAL traversal function for all pages.
        The previous `self.traverse_all` function is merely an abstraction to get around
        scrapy's async workflow.

        """
        logger.info(f'Extracting data from pages 1 -> {self.max_page}')
        for page in range(1, self.max_page + 1):
            yield self.traverse(page=page, callback=self._on_traversal_attempt, priority=-page)

    def traverse(self, page, callback, **kwargs):
        """Traverse one paginated table, given `start_date` and `end_date` attributes.

        Args:
            page (int):
            callback (callback):

        Returns:
            scrapy.FormRequest:
        """
        return FormRequest(
            url=self._response.url,
            method='POST',
            formdata=self.form(page=page),
            callback=callback,
            meta={'page': page},
            dont_filter=True,
            **kwargs,
        )

    def form(self, page):
        """Build a POST form for getting paginated data.

        Args:
            page (int):

        Returns:
            Dict[str, str]:
        """
        _form = {
            '__ASYNCPOST': 'true',
            '__EVENTVALIDATION': self._eventvalidation,
            # '__LASTFOCUS': '',
            '__VIEWSTATE': self._viewstate,
            # 'ctl00$ContentPlaceHolder1$ddlAgenciaMaritima': '-1',
            # 'ctl00$ContentPlaceHolder1$ddlCantidadCargaMovilizada': '0',
            # 'ctl00$ContentPlaceHolder1$ddlPaisCargue': '-1',
            # 'ctl00$ContentPlaceHolder1$ddlPaisDescargue': '-1',
            # 'ctl00$ContentPlaceHolder1$ddlProducto': '-1',
            # 'ctl00$ContentPlaceHolder1$ddlPuertoCargue': '-1',
            # 'ctl00$ContentPlaceHolder1$ddlPuertoDescargue': '-1',
            'ctl00$ContentPlaceHolder1$ibtnConsultar.x': '15',
            'ctl00$ContentPlaceHolder1$ibtnConsultar.y': '19',
            'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel1|'
            'ctl00$ContentPlaceHolder1$ibtnConsultar',
            'ctl00$ContentPlaceHolder1$txtFechaFinal': self.end_date,
            'ctl00$ContentPlaceHolder1$txtFechaInicial': self.start_date,
            # 'ctl00$ContentPlaceHolder1$txtNombreNave': '',
            # 'ctl00$ContentPlaceHolder1$txtOMI': '',
        }
        if page != 1:
            _form.update(
                __EVENTARGUMENT=f'Page${page}',
                __EVENTTARGET='ctl00$ContentPlaceHolder1$dgdReporteFletamento',
            )
            _form.pop('ctl00$ContentPlaceHolder1$ibtnConsultar.x')
            _form.pop('ctl00$ContentPlaceHolder1$ibtnConsultar.y')

        return _form

    def _on_traversal_attempt(self, response):
        """On traversal attempty, do the following.

        Args:
            response (scrapy.Response):

        Returns:
            callback:
        """
        logger.info(f'Traversing page: {response.meta["page"]}')
        # check for errors and retry, if any
        if not self._is_valid_response(response):
            return self.traverse(
                page=response.meta['page'],
                callback=self._on_traversal_attempt,
                priority=-response.meta['page'],
            )

        # store updated aspx states
        self._update_aspx_states(response)
        return self.on_traversal(response)

    def _update_aspx_states(self, response):
        """Update current state of the session's form, given a response.

        We need to store aspx states, since they necessary for retreiving data and tracking
        our progress when traversing paginated tables.

        Args:
            response (scrapy.Response):
        """
        logger.debug('Updating ASP.NET states')
        if not self._viewstate and not self._eventvalidation:
            self._viewstate = response.xpath('//input[@id="__VIEWSTATE"]/@value').extract_first()
            self._eventvalidation = response.xpath(
                '//input[@id="__EVENTVALIDATION"]/@value'
            ).extract_first()
        else:
            _v_match = re.search(r'__VIEWSTATE\|(\S+)\|8\|', response.text)
            _e_match = re.search(r'__EVENTVALIDATION\|(\S+)\|0\|async', response.text)
            if not _v_match or not _e_match:
                raise ValueError('Unable to extract ASP.NET states, resource has likely changed')

            self._viewstate = _v_match.group(1)
            self._eventvalidation = _e_match.group(1)

        # mainly for debugging purposes (md5 hash is used since actual string is too long)
        logger.debug(
            'Updated ASP.NET states (md5-encoded):\n'
            f'{get_md5(self._viewstate)}, {get_md5(self._eventvalidation)}'
        )

    def _is_valid_response(self, response):
        """Check and validate if response contains an error from server.

        This is necessary since the source still returns HTTP 200 despite the server
        returning an error response body.

        ArgsL
            response (scrapy.Response):

        Returns:
            bool: True if no errors
        """
        if 'frmError' in response.text:
            if self.req_retries <= 0:
                logger.error('Server returned too many errors, stopping spider')
                return

            logger.warning(
                'Server returned an internal error response, '
                f'retrying ({self.req_retries} tries left)'
            )
            self.req_retries -= 1

        return 'frmError' not in response.text
