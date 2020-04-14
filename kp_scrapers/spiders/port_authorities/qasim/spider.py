import datetime as dt

from scrapy import Spider
from w3lib.html import remove_tags

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.qasim import normalize


class QasimSpider(PortAuthoritySpider, Spider):
    name = 'Qasim'
    provider = 'Qasim'
    version = '2.0.0'
    produces = [DataTypes.PortCall]

    start_urls = [
        # berthed vessels
        'http://www.pqa.gov.pk/pqa/portoperation/BerthWiseCargoHandling',
        # expected vessels
        'http://www.pqa.gov.pk/pqa/portoperation/ExpectedShipArrivalAtOuterAnchorage',
        # arrived vessels
        'http://www.pqa.gov.pk/pqa/portoperation/ShipsAtOuterAnchorage',
        # daily shipping program, commented out as information overlaps with the above
        # 'http://www.pqa.gov.pk/pqa/portoperation/DailyShippingProgram',
    ]

    def parse(self, response):
        """Extract data from vessel movement reports.

        Args:
            response (scrapy.Response):

        Returns:
            Dict[str, str]:

        """
        # memoise reported date so it won't have to be repeatedly later
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        )

        if 'berth' in str(response).lower():
            tables = response.xpath('//table[@class="table table-responsive small"]')
        else:
            tables = response.xpath('//table[@class="table-responsive small"]')

        for table in tables:
            header = [
                may_strip(remove_tags(h_cell)) for h_cell in table.xpath('./thead//th').extract()
            ]
            data = table.xpath('./tbody//tr')
            prev_row = None

            for idx, row in enumerate(data):
                row = [
                    remove_tags(may_strip(r_cell)) if remove_tags(may_strip(r_cell)) else ''
                    for r_cell in row.xpath('./td').extract()
                ]

                # a full row contains 12 cells, however some rows are merged via the
                # berth, in which case
                if len(row) == 11:
                    row.insert(0, '')

                if not row[0]:
                    row = self.combine_row(prev_row, row)

                prev_row = row

                # sanity check in case source changes table structure
                if len(header) == len(row):
                    raw_item = {head: row[cell_idx] for cell_idx, head in enumerate(header)}
                    # contextualise raw item with meta info
                    raw_item.update(
                        port_name=self.name,
                        provider_name=self.provider,
                        reported_date=reported_date,
                        url=str(response),
                    )
                    yield normalize.process_item(raw_item)
                else:
                    self.logger.warning(f'Length of row not equal to header, discarding {row}')

    @staticmethod
    def combine_row(p_row, c_row):
        """Combine rows

        Args:
            p_row (List[str]): previous row
            c_row (List[str]): current row

        Returns:
            List[str]

        """
        combined_list = []
        for c_idx, c_cell in enumerate(c_row):
            if c_cell:
                combined_list.append(c_cell)
            else:
                combined_list.append(p_row[c_idx])

        return combined_list
