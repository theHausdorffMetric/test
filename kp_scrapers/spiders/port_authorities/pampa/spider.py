import datetime as dt

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.persist import PersistSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.pampa import normalize, parser


class PampaMelchoritaSpider(PortAuthoritySpider, PersistSpider):
    name = 'PampaMelchorita'
    provider = 'PampaMelchorita'
    version = '2.1.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://www.perupetro.com.pe/exporta/relacion_ingles.jsp']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # get persistence state
        self.portcalls_extracted = self.persisted_data.get('portcalls_extracted', [])

    def parse(self, response):
        """Entrypoint for parsing Pampa Melchorita port website.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        # memoise reported_date so it won't need to be called repeatedly
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        )

        for idx, row in enumerate(response.xpath('//table/tr')):
            # first two rows contains nested headers; ignore
            if idx in [0, 1]:
                continue

            # use cell indexes as header
            header = [str(cell) for cell in range(len(row.xpath('./td')))]

            # don't re-scrape data that was seen before
            # uniqueness defined by row ID, shipping date, vessel name, and destination
            row_hash = parser.naive_list_hash(row.xpath('./td//text()').extract(), 1, 2, 3, 4)
            if not self._check_persistence(row_hash):
                continue

            # extract data rows from table
            raw_item = row_to_dict(row, header)
            # contextualise raw_item with meta info
            raw_item.update(
                port_name='Melchorita', provider_name=self.provider, reported_date=reported_date
            )

            yield normalize.process_item(raw_item)

    def _check_persistence(self, data):
        """Check if the data to be processed has been scraped previously.

        TODO could be made generic

        Args:
            data(str):

        Returns:
            bool: True if data has not been scraped previously, else False

        """
        if data in self.portcalls_extracted:
            self.logger.debug('Portcall already extracted previously: %s', data)
            return False

        # save persistence state
        self.portcalls_extracted.append(data)
        self.persisted_data.update(portcalls_extracted=self.portcalls_extracted)
        self.persisted_data.save()

        return True
