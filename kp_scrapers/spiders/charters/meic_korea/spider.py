import logging
import random
import re

from scrapy import FormRequest, Request

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.settings.network import USER_AGENT_LIST
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.meic_korea import normalize


logger = logging.getLogger(__name__)


class MEICKoreaSpider(CharterSpider, PdfSpider):
    name = 'MC_KoreaDry_Fixtures'
    provider = 'MEIC'
    version = '1.0.1'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    start_urls = (
        # get session cookies first
        'https://www.meic.kr/',
    )

    tabula_options = {'--guess': [], '--pages': ['all'], '--stream': []}

    def __init__(self, username=None, password=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._username = username
        self._password = password

    @property
    def auth_url(self):
        # actual URL: https://meic.kr/MEMBER/login
        return 'https://www.meic.kr/j_spring_security_check'

    @property
    def auth_form(self):
        return {'j_username': self._username, 'j_password': self._password}

    @property
    def auth_headers(self):
        return {
            'Host': 'www.meic.kr',
            'User-Agent': random.choice(USER_AGENT_LIST),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/x-www-form-urlencoded',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Referer': 'https://www.meic.kr/MEMBER/login',
            'Upgrade-Insecure-Requests': '1',
        }

    @property
    def download_url(self):
        return 'https://www.meic.kr/board/attach/downloadAll?BOARD_CD={file_id}'

    def parse(self, response):
        """Entrypoint of MC_KoreaDry_Fixtures spider."""
        if not self._username:
            # There may be a possibility, the reports can be downloaded without logging in.
            # Currently the source doesn't allow that, if in future such capabilities arises we can
            # add the code over here.
            logger.warning('Login information needed to run the spider')
            return

        yield FormRequest(
            url=self.auth_url,
            headers=self.auth_headers,
            formdata=self.auth_form,
            callback=self.download_pdf,
        )

    def download_pdf(self, response):
        """Handle downloading of files after logging in."""
        if response.url.strip() != self.start_urls[0]:
            logger.error('Either resource has changed, or credentials are invalid')
            return

        valid_reports = response.xpath(
            "//a[contains(@title,'Dry Bulk') and contains(@title, 'English')]"
        )

        for report in valid_reports:
            ref = report.xpath('.//@onclick').extract_first()
            match = re.search(r'\(\s*\'\s*(?P<file_id>[A-z0-9]+)\s*\'\s*\)', ref)
            if match:
                yield Request(
                    url=self.download_url.format(file_id=match.group('file_id')),
                    method='GET',
                    callback=self.parse_pdf,
                )

    def parse_pdf(self, response):
        """Parse the downloaded file.

        Args:
            scrapy.Response:

        Yields:
            Dict[str, str]:

        """
        match = re.search(
            r'filename=(?P<filename>.+)',
            response.headers.get('Content-Disposition').decode('utf-8'),
        )

        reported_date = None
        if match:
            filename = match.group('filename')
            new_filename = filename.replace('%20', ' ')
            date_match = re.search(r'(?P<date>\d{4}-\d{1,2}-\d{1,2})', new_filename)
            if date_match:
                reported_date = date_match.group('date')

        table = self.extract_pdf_io(response.body, **self.tabula_options)
        PROCESSING_STARTED = False
        for row in table:
            # 'TERMS' and 'VSL NAME' represents the table containing fixture information
            if 'TERMS' in row and 'VSL NAME' in row:
                PROCESSING_STARTED = True
                header_row = [may_strip(val).lower() for val in row if val]
                continue

            if PROCESSING_STARTED:
                table_row = [may_strip(val) for val in row if val]
                # 'Routes denotes the end of Table'
                if 'Routes' in row:
                    PROCESSING_STARTED = False
                # By Default valid table will have only 8-10 columns and
                # we want only the last 7 columns
                elif len(table_row) in [8, 9, 10]:
                    table_row = table_row[-8:]
                    raw_item = dict(zip(header_row[-8:], table_row[-8:]))
                    raw_item.update(
                        {'provider_name': self.provider, 'reported_date': reported_date}
                    )
                    yield normalize.process_item(raw_item)
