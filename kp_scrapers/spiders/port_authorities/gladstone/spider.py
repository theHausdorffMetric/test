import datetime as dt
import json

from scrapy import Request, Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.gladstone import normalize, parser


class GladstoneSpider(PortAuthoritySpider, Spider):
    name = 'Gladstone'
    provider = 'Gladstone'
    version = '1.0.1'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
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
                'dataSource': None,
                'filterName': 'Next 7 days',
                'metaVersion': 0,
                'parameters': [
                    {'aoValues': [{'Value': -1}], 'iValueType': 0, 'sName': 'DOMAIN_ID'}
                ],
                # special key for requesting port activity table
                'reportCode': 'MSQ-WEB-0001',
                'token': None,
            }
        )

    def start_requests(self):
        """Collect a session cookie necessary for POSTing forms later.

        Yields:
            Request:

        """
        yield Request(url=self.start_urls[0], callback=self.post_form)

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
            url=self.start_urls[1],
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
        for row in json.loads(response.body)['d']['Tables'][0]['Data']:
            raw_item = {str(idx): cell for idx, cell in enumerate(row)}
            # NOTE vessel_type is located at the 5th element of the row
            if parser.is_relevant_vessel(row[4]):
                # NOTE internal vessel_id is located at the penulimate element of the row
                yield self.get_vessel_attributes(row[-2], raw_item=raw_item)

    def get_vessel_attributes(self, vessel_id, **meta):
        """Get embedded html containing vessel imo.

        Args:
            vessel_id (int): unique internal vessel_id by Queensland Govt.
            **meta: additional meta to append to response

        Returns:
            scrapy.Request:

        """
        return Request(
            url=self.start_urls[2].format(bargs=parser._vessel_bargs(vessel_id)),
            callback=self.parse_row,
            meta=meta,
        )

    def parse_row(self, response):
        _vessel_imo = parser.extract_vessel_imo(response)
        if not _vessel_imo:
            self.logger.warning(f"No IMO found for vessel {response.meta['raw_item'][3]}, skipping")
            return

        # populate with meta info
        response.meta['raw_item']['imo'] = _vessel_imo
        response.meta['raw_item']['port_name'] = self.name
        response.meta['raw_item']['provider_name'] = self.provider
        response.meta['raw_item']['reported_date'] = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        )

        return normalize.process_item(response.meta['raw_item'])
