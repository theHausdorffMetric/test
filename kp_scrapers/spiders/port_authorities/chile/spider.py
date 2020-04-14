import datetime as dt
import json

from scrapy import Request, Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.chile import normalize


class ChileanPortsSpider(PortAuthoritySpider, Spider):
    name = 'ChileanPorts'
    provider = 'DIRECTEMAR'
    version = '1.0.1'
    produces = [DataTypes.PortCall, DataTypes.Vessel]

    # human-friendly url: https://sitport.directemar.cl/#/recalando
    start_url = 'https://orion.directemar.cl/sitport/back/users/consultaNaveRecalando'

    def __init__(self, port_name, *args, **kwargs):
        """Init ChileanPorts spider.

        Website provides a choice of ports to scrape vessel movements.
        For example, to scrape portcall data for Quintero port:

            - scrapy crawl ChileanPorts -a port_name="Quintero"

        TODO add ability to scrape multiple ports in a single job

        """
        super().__init__(*args, **kwargs)
        self.filter_name = port_name

        # memoise reported_date since source doesn't provide any
        self.reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
        )

    @property
    def headers(self):
        return {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-GB,en;q=0.7,fr;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'DNT': '1',
            'Host': 'orion.directemar.cl',
            'Origin': 'https://sitport.directemar.cl',
            'Pragma': 'no-cache',
            'Referer': 'https://sitport.directemar.cl/',
        }

    def start_requests(self):
        """Entrypoint of Chile spider.

        Yields:
            scrapy.Request:

        """
        yield Request(url=self.start_url, method='POST', headers=self.headers, callback=self.parse)

    def parse(self, response):
        json_response = json.loads(response.text)
        for record in json_response.get('recordset') or []:
            # append meta and filter info
            record.update(
                filter_name=self.filter_name,
                provider_name=self.provider,
                reported_date=self.reported_date,
            )
            yield normalize.process_item(record)
