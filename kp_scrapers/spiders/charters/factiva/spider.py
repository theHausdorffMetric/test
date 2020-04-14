from datetime import datetime
import re

from dateutil.parser import parse as parse_date
from scrapy import FormRequest, Request, Spider

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.factiva import api, normalize


REPORT_START_SIGN = ['VLCC', 'SMAXES', 'AFRAMAX']
REPORT_END_SIGN = 'RELEASED'
DATA_ROW_MIN_LEN = 30


class FactivaSpider(CharterSpider, Spider):
    name = 'RS_Factiva_Fixtures'
    provider = 'Reuters'
    version = '1.0.1'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    start_urls = [
        # factiva login page
        'https://global.factiva.com/factivalogin/login.asp?productname=global',
        # oauth authentication page
        'https://auth.accounts.dowjones.com/usernamepassword/login',
        # oauth login page
        'https://global.factiva.com/factivalogin/login.asp?',
        # search for articles
        'https://snapshot.factiva.com/Search/SSResults',
        # article page
        'https://snapshot.factiva.com/AJAX/SSArticle',
    ]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    reported_date = None

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)

        self._date = kwargs.get('date', None)
        if self._date:
            self._date = parse_date(self._date).strftime('%d %b %Y')

    def start_requests(self):
        """Entry point of RS_Factiva_Fixtures spider to crawl Factiva website.

        Returns:
            Request: request for login, redirect to oauth

        """
        yield Request(url=self.start_urls[0], callback=self.do_authentication)

    def do_authentication(self, response):
        """Authenticate with user credentials.

        Args:
            response (Response):

        Returns:
            FormRequest: with user credentials

        """
        yield FormRequest(
            url=self.start_urls[1], formdata=api.oauth_form(response.url), callback=self.redirect
        )

    def redirect(self, response):
        """Submit form to login.

        Args:
            response (Response):

        Returns:
            FormRequest:

        """
        yield FormRequest.from_response(response, callback=self.do_login)

    def do_login(self, response):
        """Oauth redirect back to main page with state code.

        Args:
            response (Response):

        Returns:
            FormRequest: with oauth state code to do the login

        """
        yield FormRequest(
            url=self.start_urls[2] + api.login_query_string(response.url),
            formdata=api.login_form(response.url),
            callback=self.main_page,
        )

    def main_page(self, response):
        """Search by key words in main page.

        Args:
            response (Response):

        Returns:
            FormRequest: search form

        """
        yield FormRequest(
            url=self.start_urls[3],
            formdata=api.search_form(self._date),
            callback=self.on_search_result,
        )

    def on_search_result(self, response):
        """In search result article list page.

        Args:
            response (Response):

        Returns:
            FormRequest:

        """
        article_form = api.article_form(
            response.xpath('//section[@class="dj_search-results-content"]')
        )
        if not article_form:
            self.logger.warning('No new articles found from yesterday.')
            return

        yield FormRequest(url=self.start_urls[4], formdata=article_form, callback=self.article_page)

    def article_page(self, response):
        """Parse and process article.

        Args:
            response (Response):

        Returns:
            SpotCharter:

        """
        body = ''.join(response.xpath('//text()').extract())

        # try to get reported date from report
        # if can not, get it from attribute
        # lastly use today as reported date
        _match_date = re.search(r'[A-Za-z]{3,9}\s*\d{1,2}\s*,\s*\d{4}', body)
        if _match_date:
            try:
                self.reported_date = parse_date(may_strip(_match_date.group().repl)).strftime(
                    '%d %b %Y'
                )
            except Exception:
                self.reported_date = datetime.now().strftime('%d %b %Y')
        else:
            if self._date:
                self.reported_date = self._date
            else:
                self.logger.warning('Use today as reported date for now')
                self.reported_date = datetime.now().strftime('%d %b %Y')

        self.logger.info(f'The reported date is: {self.reported_date}')

        start_processing = False
        for raw_row in body.upper().splitlines():
            if any(sign in raw_row for sign in REPORT_START_SIGN):
                start_processing = True

            if REPORT_END_SIGN in raw_row:
                return

            if start_processing:
                # catch and split the row by regex
                row = self.split_row(raw_row)

                # if not captured by regex
                row = self.detect_data_row(raw_row) if not row else row

                if row:
                    raw_item = {str(idx): cell for idx, cell in enumerate(row)}
                    raw_item.update(self.meta_field)

                    yield normalize.process_item(raw_item)

    @staticmethod
    def split_row(raw_row):
        """Split the row and restore the cells.

        Row examples:
            - PU TO SAN 270 AG/CHINA 04/11 RNR PETROCHINA
            - EAGLE SAN FRANCISCO 130FO R0TT/SPORE 04/11 3.3M$ BP
            - STENA SUNRISE 130 ROTT/SPORE 31/10 3.3M$
            - COSGLAD LAKE 270 AG/ULSAN 03-05/11 W72.5 SK
            - NORDROSE 100FO KLAIPEDA/UKC 27-28/10 W102.5 VITOL - FLD


        Args:
            raw_row (str):

        Returns:
            Tuple[str]:

        """
        pattern = (
            # vessel name
            r'(.+?)\s'
            # size, cargo (optional)
            r'(\d{2,3})([A-Z]+)?'
            # voyage
            r'(.+?)\s'
            # lay can
            r'([^a-zA-Z]+|DNR)\s'
            # rate value
            r'([^\s]+)\s'
            # charterer, status (optional)
            r'(.+)?$'
        )

        _match = re.match(pattern, may_strip(raw_row))
        return _match.groups() if _match else None

    def detect_data_row(self, raw_row):
        """Detect data rows not captured by regex.

        Strategy:
            - Check the row length
            - Check if it matches the `vessel name / size` pattern

        As usually the pattern is not matched due to some fields are too long and joint together,
        or some fields are missing, in this case, we only extract vessel name field, and manual
        corrected later.

        Args:
            raw_row (str):

        Returns:
            Tuple[str]:

        """
        row = may_strip(raw_row)
        if len(row) >= DATA_ROW_MIN_LEN:
            _match = re.match(r'(.+?)\s\d{2,3}.+', row)
            if _match:
                self.logger.warning(f'We might miss the row: {row}')
                return _match.groups()

    @property
    def meta_field(self):
        return {'provider_name': self.provider, 'reported_date': self.reported_date}
