import datetime as dt
import logging
import json

from scrapy import Spider

from kp_scrapers.lib.parser import may_strip, row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.benlines import normalize
from parse import compile

from kp_scrapers.spiders.port_authorities.benlines import schema

logger = logging.getLogger(__name__)

tabula_options_1 = {'--stream': [], '--pages': ['all']}
tabula_options_2 = {'--guess': [], '--stream': [], '--pages': ['all'], '--lattice': []}


class BenlinesSpider(PortAuthoritySpider, PdfSpider):
    name = 'BenlinesVadinar'
    provider = 'BenlinesVadinar'
    version = '1.3.1'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['https://benline.co.in/VPR/DailyReport/']

    def parse(self, response):
        """
        Args:
            response (scrapy.HtmlResponse):

        Yields:
            Dict[str, str]:

        """
        # memoise reported_date so it won't need to be called repeatedly
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        )

        logger.info(f"reported date : {reported_date}")
        yield from self.extract_from_vessels_forecast(response.body)

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
        return port_names

    def extract_port_data(self, data):
        port_names_iter = iter(p for p in self.extract_port_names(data))
        all_rows = self.extract_pdf_io(data, **tabula_options_2)
        current_port_name = None
        current_table_id = None
        output = {}

        row_iter = (r for r in all_rows)
        for row in row_iter:
            id = schema.table_id_of_label(row[0])
            if id and id != current_table_id:
                current_table_id = id
                #print(f">>> table is now {current_table_id}")

                if id == schema.BenlineTableEnum.LOADING:
                    current_port_name = next(port_names_iter)
                    output[current_port_name] = {}
                    print(f">>> port is now {current_port_name} ; {len(output.keys())}")

                output[current_port_name][current_table_id] = []
                next(row_iter)
                continue

            #print(row)
            output[current_port_name][current_table_id].append(row)


        #print(output)

        return output
