import datetime as dt

from scrapy import Spider
from w3lib.html import remove_tags

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.milford_vts import normalize


XPATH_HEADER = '//div[@id="cphBody_Report_grid"]//th/text()'
XPATH_TABLE = '//div[@id="cphBody_Report_grid"]//tr'


class MilfordVTSSpider(PortAuthoritySpider, Spider):
    name = 'MilfordVTS'
    # analysts prefer to have a different provider than that employed in Milford spider
    # for market reasons, see https://kpler.slack.com/archives/CABLTJ468/p1574423270057800?thread_ts=1572259243.027700&cid=CABLTJ468 # noqa
    provider = 'Milford VTS'
    version = '1.0.2'
    produces = [DataTypes.PortCall, DataTypes.Vessel]

    start_urls = ['https://vts.mhpa.co.uk/Default.aspx?id=14']

    def parse(self, response):
        """Entrypoint for parsing website response.

        Args:
            response (scrapy.Response):

        Yields:
            event (Dict[str, str]):

        """
        # memoise reported date since source does not provide any
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
        )

        header = [may_strip(head) for head in response.xpath(XPATH_HEADER).extract()]

        # extract tabular data on vessel movements
        for idx, row in enumerate(response.xpath(XPATH_TABLE)):
            # first four rows are irrelevant and contain nonsense
            if idx < 5:
                continue

            record = [may_strip(remove_tags(cell)) for cell in row.xpath('.//td').extract()]
            if len(record) != len(header):
                continue

            raw_item = dict(zip(header, record))
            # contextualise raw item with meta info
            raw_item.update(
                port_name='Milford', provider_name=self.provider, reported_date=reported_date
            )
            yield normalize.process_item(raw_item)
