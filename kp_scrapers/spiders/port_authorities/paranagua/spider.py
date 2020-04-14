from scrapy import Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.paranagua import normalize, parser


ACCEPTED_HEADER_COL_COUNTS = 2


class ParanaguaLineUpSpider(PortAuthoritySpider, Spider):
    name = 'Paranagua'
    provider = 'Paranagua'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relEmitirLineUp']

    def parse(self, response):
        """Request dynamic URL containing port report within home page.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        # memoise reported date so we don't need to extract it repeatedly later on
        reported_date = response.xpath('(//td[@class="rptNormal"]//text())[2]').extract_first()

        tables = response.css('form').css('table')
        for table in tables:
            header_rows = table.css('thead tr')
            # safeguard against case where header structure may change
            # there should be two "header" rows, but we only want the second row
            if len(header_rows) != ACCEPTED_HEADER_COL_COUNTS:
                continue

            # For every table take the header row out, this will be the `raw_item` key
            header = [''.join(col.css('th::text').extract()) for col in header_rows[1].css('th')]

            # store previous row in case next row has more than 1 sub row
            row_cache = []

            table_body = table.css('tbody')
            for row in table_body.css('tr'):
                row = [''.join(col.css('td::text').extract()) for col in row.css('td')]
                # check to match number of Header columns and Table columns
                # if not, current row is a continuation of previous row
                if len(header) != len(row):
                    if len(header) > len(row):
                        for i in range(0, len(header) - len(row)):
                            row.insert(i, row_cache[i])
                    # safeguard against case where header row is shorter than table row;
                    # this is an unexpected scenario; we should log an error and continue
                    else:
                        self.logger.error(
                            "Row contains more columns than header;"
                            "table structure may have changed"
                        )
                        continue

                # memoise previous row in case next row is a continuation of current row
                row_cache = row

                raw_item = dict(
                    zip(
                        parser.remove_unwanted_space_characters(header),
                        parser.remove_unwanted_space_characters(row),
                    )
                )
                raw_item.update(
                    port_name=self.name, provider_name=self.provider, reported_date=reported_date
                )

                yield normalize.process_item(raw_item)
