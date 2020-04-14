from datetime import datetime
import os
import re

from scrapy.spiders import Request

from kp_scrapers.lib.parser import OCCIDENTAL_ENCODING
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.tuxpan import normalize, parser
from kp_scrapers.spiders.port_authorities.tuxpan.parser import REPORT_DATE_PATTERN, SPANISH_TO_MONTH


class TuxpanSpider(PortAuthoritySpider, PdfSpider):
    name = 'Tuxpan'
    version = '2.0.0'
    provider = 'Tuxpan'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_url = 'https://www.puertotuxpan.com.mx/wp-content/uploads/{}/{}/ActadeProgramacion.pdf'

    tabula_options = {'--guess': [], '--pages': ['all'], '--lattice': []}

    reported_date = None
    current_date = datetime.utcnow()

    def start_requests(self):
        yield Request(
            url=self.start_url.format(self.current_date.year, self.current_date.strftime('%m')),
            callback=self.parse,
        )

    def parse(self, response):
        """Entry point of TuxpanSpider.

        Args:
            response (Response):

        Returns:
            Dict[str, Any]:

        """
        table = self.extract_pdf_table(response, lambda x: x, **self.tabula_options)
        self.reported_date = self.extract_reported_date(response)

        for raw_item in parser.parse_table(table):
            raw_item.update(self.meta_fields)

            yield normalize.process_item(raw_item)

    def extract_reported_date(self, response):
        """Wrapper function to extract reported date from a textified pdf from a remote pdf resource

        TODO modify `bases/pdf.py` so that we do not have to call `save_file` each time we want
        to use `pdf_to_text` since it is inefficient

        Args:
            response (HtmlResponse): scrapy response

        Returns:
            str | None: date/time string in ISO format if reported date found, else None

        """
        self.save_file(self.provider, response.body)
        pdf_texts = self.pdf_to_text(
            os.path.join(self.data_path, self.provider), encoding=OCCIDENTAL_ENCODING
        ).split('\n')

        # check for date patterns in textified pdf
        for row in pdf_texts:
            date_match = re.match(REPORT_DATE_PATTERN, row)
            if date_match:
                day, raw_month, year_hour = date_match.group(0).split('DE')
                trash, trash_1, day = day.strip().split()

                year, hour_minute = re.match(
                    r'(\d+)[A-z\s]+([0-9]+\:[0-9]+).*', year_hour.strip()
                ).groups()
                hour, minute = hour_minute.split(':')
                month = SPANISH_TO_MONTH[raw_month.strip().lower().replace(' ', '')]
                return datetime(
                    int(year.strip()),
                    month,
                    int(day.strip()),
                    int(hour.strip()),
                    int(minute.strip()),
                ).isoformat()

        # return null if no reported date found
        self.logger.warning(
            'no reported date found to match regex pattern {}'.format(REPORT_DATE_PATTERN)
        )
        return None

    @property
    def meta_fields(self):
        return {
            'port_name': self.name,
            'provider_name': self.provider,
            'reported_date': self.reported_date,
        }
