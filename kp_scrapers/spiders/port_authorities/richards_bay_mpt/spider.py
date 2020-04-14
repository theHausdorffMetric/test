import re

from dateutil.parser import parse as parse_date
from scrapy.spiders import Request

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import is_number
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.richards_bay_mpt import normalize


class RichardsBayMPTSpider(PortAuthoritySpider, PdfSpider):
    name = 'RichardsBayMPT'
    provider = 'Richards Bay MPT PA'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        # main page
        'https://www.transnet.net/TU/Pages/home.aspx'
    ]

    tabula_options = {
        '--pages': ['all'],  # extract all pages
        '--lattice': [],  # when pdf table has cell borders
    }

    def parse(self, response):
        """Extract data from PDF report from source website.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        file_name = None
        javascript_str = response.xpath(
            "//script[re:match(text(), 'RICHARDS BAY TERMINAL BAR CHART')]//text()"
        ).extract()
        _match = re.search(
            r'\"FileLeafRef\": \"(RICHARDS BAY TERMINAL BAR CHART.*?.pdf)\",', str(javascript_str)
        )
        if _match:
            self.reported_date = (
                _match.group(1).replace('RICHARDS BAY TERMINAL BAR CHART', '').replace('.pdf', '')
            )
            file_name = _match.group(1).replace(' ', '%20')

        if file_name:
            yield Request(
                url=f'https://www.transnet.net/TU/Berthing/{file_name}', callback=self.parse_pdf,
            )
        else:
            self.logger.error('file does not exist, link could have been modified')

    def parse_pdf(self, response):
        """Extract data from report,  606-ETA is one section consisting of 3 columns, therefore
        the increment is 3. There are 13 sections in total to process. Hence, the final area
        of interest is 41

        Date | Time  | Day | 606                      | LOA |  ETA    | 607 | LOA | ETA | ...
        ---------------------------------------------------------------------------------------
        5    | 06:00 | Wed | Ultra Wollongong = 10000 | 200 |  01-Feb |
             | 14:00 |     | Ferro Chrome             |     |         |
             | 22:00 |     | Ferro Chrome             |     |         |

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        table = self.extract_pdf_io(response.body, **self.tabula_options)
        start_processing = False
        start_index_of_interest = 3
        final_index_of_interest = 41
        prev_vessel_item = None
        cargo_list = []
        while start_index_of_interest < final_index_of_interest:
            prev_month_item = None
            for idx, row in enumerate(table):
                # detect relevant row to start processing
                if 'TimeDay' in ''.join(row):
                    start_processing = True
                    continue

                if not start_processing:
                    continue

                # memoise day as subsequent cells are empty until the next day cell is
                # filled
                if row[0]:
                    prev_day_item = row[0]
                    prev_month_item = (
                        row[0] if len(str(row[0]).split('-')) == 3 else prev_month_item
                    )

                batch = row[start_index_of_interest : start_index_of_interest + 3]

                # detect vessel row when the loa column is filled with appropriate number
                if is_number(may_strip(batch[1])):
                    # memoise vessel details
                    vessel_item = {
                        'vessel_name': batch[0],
                        'vessel_loa': batch[1],
                        'eta': batch[2],
                        'berthed_day': prev_day_item if not row[0] else row[0],
                        'berthed_month': prev_month_item,
                        'berthed_time': row[1],
                        'port_name': 'Richards Bay',
                        'provider_name': self.provider,
                        'reported_date': parse_date(
                            may_strip(self.reported_date), dayfirst=True
                        ).isoformat(),
                    }
                    cargo_list = []
                    continue

                # cargo appears after the vessel row is identified
                # append until the next vessel row is detected
                cargo_list.append(batch[0])
                vessel_item.update(cargo_list=cargo_list,)

                # once the last index is reached, restart the processing
                if idx == len(table) - 1:
                    start_processing = False

                # yield item once cargo list is complete
                if prev_vessel_item and (
                    vessel_item.get('vessel_name') != prev_vessel_item.get('vessel_name')
                ):
                    yield normalize.process_item(prev_vessel_item)

                # since this is based on previous and current item comparison to yield,
                # we need to force yield the last item in the last section since there
                # is nothing to compare
                if start_index_of_interest >= 39 and idx == len(table) - 1:
                    yield normalize.process_item(vessel_item)

                prev_vessel_item = vessel_item

            # process next batch in the table
            start_index_of_interest += 3
