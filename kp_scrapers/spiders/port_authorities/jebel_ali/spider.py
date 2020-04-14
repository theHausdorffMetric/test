import datetime as dt

from scrapy import Spider

from kp_scrapers.lib.parser import may_strip, row_to_dict
from kp_scrapers.lib.request import allow_inline_requests
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.jebel_ali import normalize
from kp_scrapers.spiders.port_authorities.jebel_ali.session import DubaiTradeSession


# exhaustive mapping of all accepted port/terminal pairs, for easier development
PORT_TERMINALS = {'J': ['GC', 'T1', 'T2', 'T3'], 'R': ['GC', 'T1']}  # jebel ali  # rashid

# exhaustive mapping of all accepted vessel movements, for easier development
VESSEL_MOVEMENTS = ['A', 'E', 'I', 'S']  # anchorage  # expected  # at berth  # sailed


class JebelAliSpider(PortAuthoritySpider, Spider):
    name = 'JebelAli'
    provider = 'JebelAli'
    version = '2.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    reported_date = (
        dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
    )

    spider_settings = {'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter'}

    def __init__(self, movement=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # NOTE this spider only scraped exclusively Jebel Ali (J) port, hardcoded for now
        # TODO scrape Rashid port (to be confirmed with product owners)
        self.terminals = PORT_TERMINALS['J']
        # default to expected vessels
        self.movement = movement or 'E'

        if self.movement not in VESSEL_MOVEMENTS:
            raise ValueError(f'Unknown vessel movement: {movement}')

        self.logger.info(f'Requesting info: port=J, movement={movement}')

    def start_requests(self):
        """Entry point of Jabel Ali spider.

        Yields:
            scrapy.Request:

        """
        for terminal in self.terminals:
            port_activity = DubaiTradeSession(
                # NOTE this spider only scraped exclusively Jebel Ali (J) port, hardcoded for now
                # TODO scrape Rashid port (to be confirmed with product owners)
                port='J',
                terminal=terminal,
                movement=self.movement,
                callback=self.parse,
            )
            yield from port_activity.traverse_all()

    @allow_inline_requests
    def parse(self, response):
        """Parse responses from source containing port activity.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        for idx, row in enumerate(response.xpath('//table[@id="vesselinfo"]//tr')):
            # first row will always contain headers
            if idx == 0:
                headers = [
                    may_strip(cell) for cell in row.xpath('th//text()').extract() if may_strip(cell)
                ]
                continue

            raw_item = row_to_dict(row, headers)
            # append extra shipping agent info
            _agent_res = yield DubaiTradeSession.get_shipping_agent(
                rotation_id=may_strip(row.xpath('.//a/text()').extract_first())
            )
            raw_item.update(**DubaiTradeSession.parse_shipping_agent(_agent_res))

            # contextualise raw item with meta info
            raw_item.update(
                port_name='Jebel Ali', provider_name=self.provider, reported_date=self.reported_date
            )
            yield normalize.process_item(raw_item)
