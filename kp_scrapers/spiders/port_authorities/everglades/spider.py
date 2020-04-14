from base64 import b64encode
import datetime as dt
import json

from scrapy import Request, Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.everglades import normalize, parser


class EvergladesSpider(PortAuthoritySpider, Spider):
    name = 'Everglades'
    provider = 'Everglades'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    auth_url = 'https://pevvesseltraffic.broward.org/webx/'  # entry point
    data_url = 'https://pevvesseltraffic.broward.org/webx/services/wxdata.svc/GetDataX'
    vessel_url = 'https://pevvesseltraffic.broward.org/webx/dashb.ashx?bargs={bargs}'

    # reverse engineered b64-decoded query string, used for collecting vessel imo
    bargs_param = 'db=standard.vesselinfo&VID={vessel_id}'

    def start_requests(self):
        """Collect an auth token necessary for POSTing forms later.

        Yields:
            Request: main url request

        """
        yield Request(url=self.auth_url, callback=self.send_form_request)

    @staticmethod
    def init_form_data(time_filter='Next 7 Days'):
        """Init form data for POSTing.

        Default time filter is 'Next 7 Days' as it gives us the most relevant data.

        Args:
            time_filter (str): accepted strings: 'Next 7 Days',
                                                 'Today',
                                                 'Tomorrow',
                                                 'Yesterday',
                                                 'Last 7 Days'

        Returns:
            str: json-formatted string

        """
        return json.dumps(
            {
                'filterName': time_filter,
                # special key for requesting port activity table
                'reportCode': 'PEV-WEB-0002',
            }
        )

    def send_form_request(self, response):
        """Post form to retrieve table containing port activity.

        Args:
            response (scrapy.response):

        Yields:
            Request: POST request

        """
        self.logger.info(
            'Auth token obtained: {}'.format(
                str(response.headers.getlist('set-cookie')[0]).partition(';')[0]
            )
        )
        yield Request(
            url=self.data_url,
            callback=self.collect_form_response,
            method='POST',
            headers={'content-type': 'application/json; charset=utf-8'},
            body=self.init_form_data(),
        )

    def send_imo_request(self, row, **meta):
        """Get embedded html containing vessel imo.

        Args:
            row (scrapy.Selector):
            meta: additional meta to append to response

        Returns:
            scrapy.Request:

        """
        return Request(
            url=self.vessel_url.format(
                bargs=b64encode(self.bargs_param.format(vessel_id=row[4]).encode()).decode()
            ),
            callback=self.collect_all_responses,
            meta=meta,
        )

    def collect_form_response(self, response):
        """Collect POST response for port activity.

        Serialise json response.
        Transform into raw dict.

        JSON response structure (as of 10 April 2018):
            "d" : {
                "Tables": [{
                    "Data": [ ... ] <<< list of port activity rows to extract
                    "MetaData":
                    "__type"":
                    "Name":
                    "AsOfDate":
                }],
                "_type":
                "ReportCode":
            }

        Example of each port actvity row (no headers are provided):
            ['Scheduled',
             '/Date(1523397600000-0400)/',
             'ARR',
             '583487',
             '519663', <<< NOTE this str is used in formatting vessel imo url
             'MAERSK MATSUYAMA',
             'SEA BUOY',
             '09',
             '5691898',
             'ISS Marine Services, Inc.',
             'Freeport',
             'Tampa, Florida',
             '540696',
             '2020',
             'GASOLINE',
             'Seabulk Towing, Inc.',
             'Captain's Choice',
            ]

        Args:
            response (scrapy.Response):

        Yields:
            something:

        """
        for row in json.loads(response.body)['d']['Tables'][0]['Data']:
            raw_item = {str(idx): cell for idx, cell in enumerate(row)}
            yield self.send_imo_request(row, raw_item=raw_item)

    def collect_all_responses(self, response):
        response.meta['raw_item']['imo'] = parser.extract_vessel_imo(response)
        response.meta['raw_item']['port_name'] = self.name
        response.meta['raw_item']['provider_name'] = self.provider
        response.meta['raw_item']['reported_date'] = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        )
        return normalize.process_item(response.meta['raw_item'])
