import datetime as dt
import logging
from typing import Any, Dict, List

from parse import compile

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.benlines import normalize
from kp_scrapers.models.utils import validate_item
from kp_scrapers.models.port_call import PortCall

logger = logging.getLogger(__name__)

tabula_options_1 = {'--stream': [], '--pages': ['all']}
tabula_options_2 = {'--guess': [], '--stream': [], '--pages': ['all'], '--lattice': []}


class BenlinesSpider(PortAuthoritySpider, PdfSpider):
    name = 'Benlines'
    provider = 'Benlines'
    version = '0.0.1'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    day = dt.datetime.utcnow().day
    month = dt.datetime.utcnow().month
    year = dt.datetime.utcnow().year

    start_urls = [f'https://benline.co.in/VPR/DailyReport/VPR-{day:02d}-{month:02d}-{year:4d}.PDF']

    def parse(self, response):
        """
        Args:
            response (scrapy.HtmlResponse):

        Yields:
            Dict[str, str]:

        """
        yield from self.extract_port_data(response.body)

    def extract_port_names(self, data):
        """Extract port names from report.
        Args:
            response (scrapy.Response):

        Returns:
            List[str]:

        """

        self.logger.info("going to loop")
        all_rows = self.extract_pdf_io(data, **tabula_options_1)

        # for row in all_rows:
        #   print(row)

        def is_port_start_row(row):
            try:
                row.index('BEN LINE AGENCIES (INDIA) PVT. LTD.')
                return True
            except:
                return False

        start_port_indexes = [index for (index, row) in enumerate(all_rows) if is_port_start_row(row)]
        pattern = compile("{} ({:d}/{:d}/{:d})")

        def f(s):
            try:
                r = pattern.parse(s)
                return r[0]
            except:
                return None

        port_names = [f for f in [f(all_rows[index + 2][0]) for index in start_port_indexes] if f]
        logger.info(f"found {len(port_names)} ports")
        return port_names

    def extract_port_data(self, data):
        port_names_iter = iter(p for p in self.extract_port_names(data))
        all_rows = self.extract_pdf_io(data, **tabula_options_2)
        current_port_name = None
        current_table_label = None
        current_table_col_names = None
        current_item = {}
        reported_date = dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

        row_iter = (r for r in all_rows)
        for row in row_iter:
            if is_row_a_table_label(row):
                if is_row_a_new_port(row):
                    current_port_name = next(port_names_iter)
                    #print(f">>> port is now {current_port_name}")
                current_table_label = row[0]
                #print(f">>> table is now {current_table_label}")
                current_table_col_names = next(row_iter)
                #print(f">>> col names {current_table_col_names}")
                continue

            # print(row)
            #logger.info(row)
            current_item = {
                'provider_name': self.provider,
                'port_name': current_port_name,
                'reported_date': reported_date,
                'table_label':current_table_label
            }
            for (k, v) in zip(current_table_col_names, row):
                current_item.update({k: v})

            yield normalize.process_item(current_item)


def is_row_a_table_label(row: List[str]) -> bool:
    """
    the parsing of the pdf file returns a list of rows
    where table name, column names and data are all rows

    Args:
        a row in the pdf file

    Returns:
        True if this row is a table label

    """
    label = row[0]
    if label == 'VESSELS AT BERTH FOR  LOADING' \
            or label == 'VESSELS AT BERTH FOR  DISCHARGE' \
            or label == 'VESSELS WAITING FOR BERTH' \
            or label == 'VESSELS EXPECTED TO ARRIVE PORT':
        return True
    return False


def is_row_a_new_port(row: List[str]) -> bool:
    """
    the parsing of the pdf file returns a list of rows
    where table name, column names and data are all rows

    Args:
        a row in the pdf file

    Returns:
        True if this row shows

    """
    label = row[0]
    if label == 'VESSELS AT BERTH FOR  LOADING':
        return True
    return False
