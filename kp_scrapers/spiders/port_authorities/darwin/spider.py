import datetime as dt
import json
import random

from scrapy import Request, Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.settings.network import USER_AGENT_LIST
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.darwin import normalize, parser


def request_body():
    """Build request body for querying Dampier's port schedule.

    Args:
        code (str | int):

    Returns:
        str: serialised json request body

    """
    body = {'reportCode': f'AUDRW-WEB-0001'}
    # different report types require different request params
    body.update(
        parameters=[
            {'sName': 'START_DATE', 'aoValues': [{'Value': _get_darwin_time()}]},
            {'sName': 'END_DATE', 'aoValues': [{'Value': _get_darwin_time(days=7)}]},
        ]
    )

    return json.dumps(body)


def _get_darwin_time(**offset):
    """Get local time at Dampier port, with optional offset.

    Args:
        offset: keyword arguments for `dt.timedelta`

    Returns:
        str: ISO-8601 formatted datetime string

    """
    return (
        dt.datetime.utcnow().replace(second=0, microsecond=0) + dt.timedelta(**offset)
    ).isoformat()


class DarwinSpider(PortAuthoritySpider, Spider):
    name = 'Darwin'
    provider = 'Darwin'
    version = '2.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        # initial webpage for obtaining session ID
        'https://portinfo.darwinport.com.au/webx/',
        # endpoint for vessel movement data
        'https://portinfo.darwinport.com.au/webx/services/wxdata.svc/GetDataX',
        # retrieve individual vessel static attributes
        'https://portinfo.darwinport.com.au/WebX/dashb.ashx?db=standard.vesselinfo&VID={v_id}',
    ]

    @property
    def headers(self):
        return {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip,deflate,br',
            'Accept-Language': 'en-GB,en;q=0.5',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json;charset=utf-8',
            'DNT': '1',
            'Referer': 'https://portinfo.darwinport.com.au/webx/',
            'Host': 'portinfo.darwinport.com.au',
            'User-Agent': random.choice(USER_AGENT_LIST),
        }

    def start_requests(self):
        """Collect a session cookie necessary for POSTing forms later.

        Yields:
            Request:

        """
        yield Request(url=self.start_urls[0], callback=self.post_form)

    def post_form(self, response):
        """Extract data from API endpoint.

        Args:
            scrapy.Response:

        Yields:

        """
        yield Request(
            url=self.start_urls[1],
            callback=self.collect_response,
            method='POST',
            body=request_body(),
            headers=self.headers,
        )

    def collect_response(self, response):
        """Collect POST response for port activity.

        Serialise json response.
        Transform into raw dict.

        JSON response structure (as of 23 May 2019):
            "d" : {
                "ReportCode":
                "Tables":
                {
                    "Name":
                    "AsOfDate":           <<< reported date; might be useful
                    "Data": [ ... ]       <<< relevant list of vessel schedules to extract
                    "IsCustomMetaData":
                    "MetaData": {
                        "Columns": {
                            "__type":
                            "Format":
                            "Name":       <<< column names are listed here
                            "SortOrder":
                            "Sortable":
                            "Template":
                            "Title":
                            "Visible":
                            "Width":
                        },
                        "Script":
                        "TemplateRow":
                        "TemplateTable":
                        "Version":
                        "__type":
                    }
                    "__type":
                },
                "__type":
            }

        Example of each vessel schedule row (headers are provided under the "Columns" key):
            ['Sun 13 Jan 2019 09:15',
             'DEP',
             'GRACE RIVER',
             'Inpex LPG',
             'SEA',
             'Darwin',
             'Hong Kong',
             'Osaka',
             'Wilhelmsen Ships Service Pty Ltd',
             563068,
             17073,
             1016,
             1,
            ]

        Args:
            response (scrapy.Response):

        Yields:
            scrapy.Request:

        """
        response_json = json.loads(response.body)

        # retrieve column names in a non-robust manner,
        # and so it allows us to be alerted quickly should the resource change
        column_names = [
            col['Name'] for col in response_json['d']['Tables'][0]['MetaData']['Columns']
        ]

        # create a raw item from each individual table row
        for row in response_json['d']['Tables'][0]['Data']:
            raw_item = {col: value for col, value in zip(column_names, row)}
            # get embedded html containing vessel IMO number
            yield Request(
                url=self.start_urls[2].format(v_id=raw_item['VESSEL_ID']),
                callback=self.parse_individual_row,
                meta={'raw_item': raw_item},
            )

    def parse_individual_row(self, response):
        vessel_imo = parser.extract_vessel_imo(response)
        if not vessel_imo:
            self.logger.info(
                f"Vessel {response.meta['raw_item']['SHIP']} has no IMO number, skipping"
            )
            return

        # contextualise raw item with meta info, and also vessel IMO
        response.meta['raw_item'].update(
            vessel_imo=vessel_imo,
            port_name=self.name,
            provider_name=self.provider,
            reported_date=dt.datetime.utcnow()
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .isoformat(),
        )
        return normalize.process_item(response.meta['raw_item'])
