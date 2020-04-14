import json

from scrapy import Request, Spider

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.gladstone_gpcl import normalize, parser


FULL_ROW_LEN = 7
SHORT_ROW_LEN = 3


class GladstoneGpclSpider(PortAuthoritySpider, Spider):
    name = 'Gladstone_GPCL'
    version = '1.0.0'
    provider = 'Gladstone'
    produces = []

    raw_items = []

    start_urls = ['http://content1.gpcl.com.au/ViewContent/ShippingList/ShippingList.aspx']

    api = [
        # landing page for getting a session cookie, also useful for debugging
        'https://qships.tmr.qld.gov.au/webx/',
        # endpoint for retreiving vessel schedules
        'https://qships.tmr.qld.gov.au/webx/services/wxdata.svc/GetDataX',
        # endpoint for retreiving vessel attributes
        'https://qships.tmr.qld.gov.au/webx/dashb.ashx?bargs={bargs}',
    ]

    spider_settings = {'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter'}

    @property
    def form_data(self):
        return json.dumps(
            {
                'filterName': 'Next 7 days',
                # special key for requesting port activity table
                'reportCode': 'MSQ-WEB-0001',
            }
        )

    def parse(self, response):
        """Entry point of Gladstone_GPCL spider.

        An item is the combination of the following three parts, they are in consecutive order:
            1. ['NSU QUEST', 'INCHCAPE SHIPPING', 'R.G.TANNA COAL TERMINAL', 'AT ANCHOR', 'Coal',
                '40300', '']
            2. ['Coal', '45500', '']
            3. ['Total', '198500', '13-Dec-18 09:46']

        Part 1 and part 3 are mandatory, so we can identify the start of an item by row length,
        the end of an item by `Total`, part 2 is optional, it means other possible cargo movement
        in the port call.

        The basic idea for this function is to combine the three parts by join each fields with `,`,
        except for last field denoting reported date, we'll use the field in part 3.

        Args:
            response (scrapy.Response):

        Returns:
            PortCall:

        """
        cur_row = []
        for raw_row in response.xpath('//tr'):
            row = [may_strip(each) for each in raw_row.xpath('.//td/text()').extract()]

            if len(row) == FULL_ROW_LEN:
                cur_row = row

            if len(row) == SHORT_ROW_LEN:
                if 'Total' not in row:
                    # join cargo and volume fields with `,`
                    for i in range(-2, -4, -1):
                        cur_row[i] += ',' + row[i]

                else:
                    if not parser.is_future_eta(cur_row[3]):
                        continue

                    # add reported date to the full vessel row
                    cur_row[-1] = row[-1]
                    raw_item = {str(idx): cell for idx, cell in enumerate(cur_row)}
                    raw_item.update(provider_name=self.provider, port_name='Gladstone')

                    self.raw_items.append(raw_item)

        # start retrieving vessel attributes from gladstone
        # (Queensland Government Maritime Safety Queensland)
        yield Request(url=self.api[0], callback=self.post_form)

    def post_form(self, response):
        """Post form to retrieve table containing vessel schedules.

        Args:
            response (scrapy.response):

        Yields:
            Request: POST request

        """
        self.logger.info(
            'Auth token obtained: {}'.format(response.headers.getlist('set-cookie')[0]).partition(
                ';'
            )[0]
        )
        yield Request(
            url=self.api[1],
            callback=self.collect_response,
            method='POST',
            headers={'content-type': 'application/json; charset=utf-8'},
            body=self.form_data,
        )

    def collect_response(self, response):
        """Collect POST response for port activity.

        Serialise json response.
        Transform into raw dict.

        JSON response structure (as of 10 December 2018):
            "d" : {
                "Tables": [{
                    "Data": [ ... ] <<< relevant list of vessel schedules to extract
                    "MetaData":
                    "__type"":
                    "Name":
                    "AsOfDate":
                    "IsCustomMetaData":
                }],
                "_type":
                "ReportCode":
            }

        Example of each vessel schedule row (no headers are provided by the endpoint):
            [75584,
             206660,
             'ARR',
             'SERI ANGKASA',
             'LIQUEFIED GAS TANKER',
             283.067,
             'Gulf Agency Company (Gladstone)',
             '/Date(1544582700000+1000)/',
             '/Date(1544595300000+1000)/',
             'Fairway Buoy Anchorage',
             'Queensland Curtis LNG',
             'PLAN',
             'Yeosu (ex Yosu)',
             'China',
             'Q2B.18.52',
             610186,
             705,
            ]

        Args:
            response (scrapy.Response):

        Yields:
            something:

        """
        data_table = json.loads(response.body)['d']['Tables'][0]['Data']

        for raw_item in self.raw_items:
            vessel_name = raw_item.get('0')

            # search for matched vessel to get vessel id
            vessel_id = None
            for entry in data_table:
                # compare with vessel name
                # index 3 denotes vessel name, index -2 denotes the vessel id we need
                # in order to build request for vessel detail page
                if vessel_name == entry[3]:
                    vessel_id = entry[-2]
                    break

            if not vessel_id:
                continue

            yield self.get_vessel_attributes(vessel_id, raw_item=raw_item)

    def get_vessel_attributes(self, vessel_id, **meta):
        """Get embedded html containing vessel imo.

        Args:
            vessel_id (int): unique internal vessel_id by Queensland Govt.
            **meta: additional meta to append to response

        Returns:
            scrapy.Request:

        """
        return Request(
            url=self.api[2].format(bargs=parser._vessel_bargs(vessel_id)),
            callback=self.parse_row,
            meta=meta,
        )

    def parse_row(self, response):
        _vessel_imo = parser.extract_vessel_imo(response)
        if not _vessel_imo:
            self.logger.warning(
                f"No IMO found for vessel {response.meta['raw_item'].get('0')}, skipping"
            )
            return

        raw_item = response.meta['raw_item']
        raw_item['imo'] = _vessel_imo

        return normalize.process_item(raw_item)
