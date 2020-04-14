import datetime as dt

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.quebec import normalize


class QuebecSpider(PortAuthoritySpider, PdfSpider):
    name = 'Quebec'
    version = '2.0.1'
    provider = 'Quebec'
    produces = [DataTypes.PortCall]

    start_urls = ['https://www.portquebec.ca/pdf/cedule_navire.pdf']

    # pdf parsing parameters
    tabula_options = {
        '-a': ['87,4,519,943'],
        '-p': ['all'],
        '-c': ['56,202,241,350,427,463,518,551,584,640,692,734,766,802,818,940'],
    }

    # 0-based column index where berth number is given
    berth_column_idx = 0

    def parse(self, response):
        """Extract data from pdf report.

        Args:
            response (scrapy.Response):

        Returns:
            Dict[str, str]:
        """
        table = self.extract_pdf_table(
            response, information_parser=lambda x: x, **self.tabula_options
        )

        is_relevant_section = False
        for row in table:
            # only extract relevant table section
            if 'SECTION2' in ''.join(row):
                is_relevant_section = True
                continue

            # don't extract irrelevant table section
            if not is_relevant_section:
                continue

            # don't extract rows without vessels
            if not row[self.berth_column_idx]:
                continue

            raw_item = {str(idx): cell for idx, cell in enumerate(row)}
            # contextualise raw item with metadata
            raw_item.update(
                reported_date=dt.datetime.utcnow()
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .isoformat(),
                port_name=self.name,
                provider_name=self.provider,
            )
            yield normalize.process_item(raw_item)
