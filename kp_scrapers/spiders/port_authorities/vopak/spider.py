from datetime import datetime

from scrapy import Spider
from scrapy.http import FormRequest, Request

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import extract_row
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.vopak import normalize


PORTS_TO_SCRAP = {
    'Amsterdam': 'AMS',
    'Antwerp': 'AWP',
    'Hamburg': 'HAM',
    'Rotterdam': 'RTM',
    'Terneuzeun': 'TNZ',
}
TERMINAL_COL = 6


class VopakSpider(PortAuthoritySpider, Spider):
    """Vopak port source contains data of:
        - Amsterdam
        - Antwerp
        - Rotterdam
        - Hamburg
        - Terneuzen

    URL changes according to the port code in PORTS_TO_SCRAP.
    """

    name = 'Vopak'
    provider = 'Vopak'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel]

    start_urls = [
        'https://portview.agencies.vopak.com/ords/f?p=PUB:1:0::::P1_OFE_CODE:AMS',
        'https://portview.agencies.vopak.com/ords/wwv_flow.ajax',
    ]

    reported_date = datetime.utcnow().isoformat(timespec='seconds')

    def start_requests(self):
        """First request to get cookie.

        Returns:
            PortCall:

        """
        yield Request(url=self.start_urls[0], callback=self.parse_port_page)

    def parse_port_page(self, response):
        """Second request to open different ports pages."""
        p_flow_id = response.xpath('//input[@id="pFlowId"]/@value').extract_first()
        p_flow_step_id = response.xpath('//input[@id="pFlowStepId"]/@value').extract_first()
        p_instance = response.xpath('//input[@id="pInstance"]/@value').extract_first()

        for port in PORTS_TO_SCRAP.keys():
            form_data = {
                "p_arg_names": "P1_OFE_CODE",
                "p_arg_values": PORTS_TO_SCRAP[port],
                "p_debug": "",
                "p_flow_id": p_flow_id,
                "p_flow_step_id": p_flow_step_id,
                "p_instance": p_instance,
                "p_request": "APXWGT",
                "p_widget_action": "reset",
                "p_widget_name": "classic_report",
                "x01": "8198948492376221",
            }
            yield FormRequest(
                url=self.start_urls[1],
                callback=self.parse,
                meta={"port": port, "dont_redirect": True, "handle_httpstatus_list": [302]},
                formdata=form_data,
            )

    def parse(self, response):
        port_name = response.meta['port']

        headers = response.xpath('//table[@class="t-Report-report"]/thead//th/@id').extract()
        for row in response.xpath('//table[@class="t-Report-report"]/tbody/tr'):
            cells = extract_row(row)
            raw_item = {headers[idx]: cell for idx, cell in enumerate(cells)}
            raw_item.update(port_name=port_name, provider_name=self.provider)

            ter_link = row.xpath('.//td[@headers="URL"]/a/@href').extract_first()
            detail_url = f'https://portview.agencies.vopak.com/ords/{ter_link}'
            yield Request(
                url=detail_url, meta={'raw_item': raw_item}, callback=self.parse_detail_page
            )

    def parse_detail_page(self, response):
        raw_item = response.meta['raw_item']

        # update call details
        call_details = response.xpath('//div[@id="report_6854648446187435_catch"]')
        cd_keys = list(map(may_strip, call_details.xpath('.//dt//text()').extract()))
        cd_values = list(map(may_strip, call_details.xpath('.//dd//text()').extract()))
        raw_item.update({cd_keys[idx]: value for idx, value in enumerate(cd_values)})

        # update terminal details, there could be more than one group of terminal details
        terminal_details = response.xpath('//div[@id="report_9311931567071502_catch"]')

        td_keys = list(map(may_strip, terminal_details.xpath('.//dt//text()').extract()))
        td_values = list(map(may_strip, terminal_details.xpath('.//dd//text()').extract()))

        # no terminal details
        if not td_keys:
            yield normalize.process_item(raw_item)

        for i in range(0, len(td_keys), TERMINAL_COL):
            raw_item.update({td_keys[i + idx]: td_values[i + idx] for idx in range(TERMINAL_COL)})
            yield normalize.process_item(raw_item)
