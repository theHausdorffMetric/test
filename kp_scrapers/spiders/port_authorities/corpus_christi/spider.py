"""CorpusChristi spider module.

Source: https://portofcc.com/capabilities/login/ (go to "Client Traffic Board/E-Wharfage")

"""
from base64 import b64encode
import datetime as dt
import json
import random
import re

from scrapy import FormRequest, Request, Spider

from kp_scrapers.lib.utils import random_delay
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.settings.network import USER_AGENT_LIST
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.corpus_christi import normalize, parser


class CorpusChristiSpider(PortAuthoritySpider, Spider):
    name = 'CorpusChristi'
    provider = 'Corpus Christi'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        # initial webpage for obtaining session ID
        'http://71.40.205.203/webx/login.aspx',
        # endpoint for vessel movement data
        'http://71.40.205.203/webx/services/wxdata.svc/GetDataX',
        # retrieve individual vessel static attributes
        'http://71.40.205.203/webx/dashb.ashx?bargs={hash}',
    ]

    def __init__(self, username, password, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # store parameters necessary to authenticate and send api requests
        self.username = username
        self.password = password
        self.api_parameters = kwargs

    def start_requests(self):
        """Login and authenticate before we can use the API."""
        body = {
            'Login:UserName': self.username,
            'Login:Password': self.password,
            'Login:LoginButton': 'Log In',
            '__EVENTARGUMENT': '',
            '__EVENTTARGET': '',
            '__EVENTVALIDATION': '/wEWBAKa75LuBwKl4b+zBQL+pMkqAq7wudEEVgaey9ArLLyxUQVhPtMRKmrdH1A=',
            '__LASTFOCUS': '',
            '__VIEWSTATE': (
                '/wEPDwULLTIwMzM2NjQyOTgPFgQeCHVzZXJuYW1lZB4IcGFzc3dvcmRkFgICAw9kFgQCAQ9kFgICBw9k'
                'FgICBQ8PFgYeBFRleHQFFFRlcm1zIGFuZCBjb25kaXRpb25zHgZUYXJnZXQFBl9ibGFuax4LTmF2aWdh'
                'dGVVcmwFE3Jlc291cmNlcy9ldWxhLmh0bWxkZAIDD2QWAgIDDxAPFgIeB0NoZWNrZWRoZGRkZBgBBR5f'
                'X0NvbnRyb2xzUmVxdWlyZVBvc3RCYWNrS2V5X18WAQUWTG9naW46TG9naW5JbWFnZUJ1dHRvbrxwMw4l'
                'mfLgF2ZPC7XZI58rUYxr'
            ),
            '__VIEWSTATEGENERATOR': 'B6400C2D',
        }
        yield FormRequest(url=self.start_urls[0], callback=self.query_api, formdata=body)

    @random_delay(average=15)
    def query_api(self, response):
        """Extract vessel movement data from API endpoint."""
        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip,deflate',
            'Accept-Language': 'en-GB,en;q=0.5',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json; charset=utf-8',
            'DNT': '1',
            'Referer': 'http://71.40.205.203/webx/Default.aspx',
            'Host': '71.40.205.203',
            'User-Agent': random.choice(USER_AGENT_LIST),
            'X-Requested-With': 'XMLHttpRequest',
        }
        form = parser.VesselMovementQuery(**self.api_parameters).to_dict()
        self.logger.debug("Form to be sent to API:\n%s", form)

        yield Request(
            url=self.start_urls[1],
            method='POST',
            callback=self.collect_response,
            headers=headers,
            body=json.dumps(form),
        )

    def collect_response(self, response):
        """Collect POST response for specified vessel movements.

        Serialise json response, and transform into raw dict.

        JSON response structure (as of 4 July 2019):
            "d" : {
                "ReportCode":
                "Tables": [{  ## list with only one element in it
                    "Name":
                    "AsOfDate":               <<< reported date; might be useful
                    "Data": [ ... ]           <<< relevant list of vessel movement to extract
                    "MetaData": {
                        "Columns": [  ## list with as many elements as there are table columns
                            {
                                "__type":
                                "Format":
                                "Name":       <<< column names are listed here
                                "SortIndex":
                                "SortOrder":
                                "Sortable":
                                "Template":
                                "Title":
                                "Visible":
                                "Width":
                            }, ...
                        ],
                        "Script":
                        "TemplateRow":
                        "TemplateTable":
                        "Version":
                        "__type":
                    }
                    "__type":
                }],
                "__type":
            }

        Example of each vessel schedule row (headers are provided under the "Columns" key):
            ['Scheduled',
             'CH1',
             'CH1',
             'MARIA ENERGY',
             'LBR',
             'GAC Shipping (USA) Inc.',
             'BB Maurico/agt 438-0307   **cleared to enter** 6/24 0746 JA',
             'LOADING',
             'E.T.A.',
             '',
             '07/06/2019 00:01:00',
             '',
             '',
             'BEST BET',
             42031,
             3586,
             None
            ],

        Args:
            response (scrapy.Response):

        Yields:
            scrapy.Request:

        """
        response_json = json.loads(response.body)

        # retrieve column names in a non-robust manner,
        # and so it allows us to be alerted quickly should the resource change
        headers = [col['Name'] for col in response_json['d']['Tables'][0]['MetaData']['Columns']]

        # create a raw item from each individual table row
        for row in response_json['d']['Tables'][0]['Data']:
            raw_item = {head: value for head, value in zip(headers, row)}
            # special rule; get source's internal vessel_id in order to obtain vessel IMO
            vessel_id = raw_item['VESSEL_ID']
            # get embedded html containing vessel IMO number
            yield Request(
                url=self.start_urls[2].format(hash=self._generate_vessel_hash(vessel_id)),
                callback=self._extract_vessel_imo,
                meta={'raw_item': raw_item},
            )

    @random_delay(average=15)
    def _extract_vessel_imo(self, response):
        """Extract vessel IMO from embedded vessel url.

        Each entry of the vessel movement table will show vessel name only.
        Vessel IMO is contained within an embedded html accessed via a link within each table entry.
        We want to extract vessel IMO for robustness of vessel identification.

        To do the above, we send a Request for each table entry to retrieve html.
        Then, we regex match for vessel IMO.

        Args:
            response (scrapy.HtmlResponse):

        Returns:
            Optional[Dict[str, Any]]:

        """
        imo_match = re.match(r'.*"LRN":"(\d{7})"', response.text, re.DOTALL)
        vessel_imo = imo_match.group(1) if imo_match else None
        if not vessel_imo:
            # source provides vessel movement data and vessel attributes in a very deliberate format
            # thus, if a vessel has no IMO number, likely it is either an internal server error,
            # or simply that the vessel is too small and not relevant for our interests
            # e.g. dredging barges and pilot vessels with no IMO number
            self.logger.info(
                f"Vessel {response.meta['raw_item']['SHIP']} has no IMO number, skipping"
            )
            return

        # contextualise raw item with meta info, and also vessel IMO
        response.meta['raw_item'].update(
            vessel_imo=vessel_imo,
            port_name='Corpus Christi',
            provider_name=self.provider,
            reported_date=dt.datetime.utcnow()
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .isoformat(),
        )
        return normalize.process_item(response.meta['raw_item'])

    @staticmethod
    def _generate_vessel_hash(vessel_id):
        return b64encode(f'db=standard.vesselinfo&VID={vessel_id}'.encode()).decode()
