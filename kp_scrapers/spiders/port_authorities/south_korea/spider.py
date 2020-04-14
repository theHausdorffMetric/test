"""South Korean ports spider.

Provider: Korean Ministry of Oceans and Fisheries
Website:  https://tinyurl.com/yax8vous (for debugging and verifying scraped data)

"""
import datetime as dt
import json

from scrapy import Request, Spider

from kp_scrapers.lib.errors import InvalidCliRun
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.request import allow_inline_requests
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.south_korea import normalize


class SouthKoreaSpider(PortAuthoritySpider, Spider):
    name = 'SouthKorea'
    provider = 'Korean MOF'
    version = '1.1.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    # lineup of vessel movements in selected port and date range, does not contain cargo data
    lineup_url = 'https://new.portmis.go.kr/portmis/sp/vssl/vsch/selectSpVsslInPagingList.do'

    # provides bol number which is required for POSTing the cargo_url form
    bol_url = 'https://new.portmis.go.kr/portmis/co/como/cnlg/selectNlgUfrgtInfoPop3TabList1.do'
    # detailed description of onboard cargo/volume/movement/charterers
    cargo_url = 'https://new.portmis.go.kr/portmis/co/como/cnlg/selectNlgUfrgtInfo.do'

    # provides dangerous goods declaration number
    danger_goods_url = 'https://new.portmis.go.kr/portmis/fr/dgst/dinu/dinu/selectDgst1010List.do'
    # detailed description of onboard dangerous cargo/volume/movement/charterers
    cargo_alt_url = 'https://new.portmis.go.kr/portmis/fr/dgst/dinu/dinu/selectDgst1010Map.do'

    spider_settings = {
        # as we're accessing the website's API, duplicate filtering has to be disabled
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
        # be kind to website, so that we don't get banned permanently
        'DOWNLOAD_DELAY': 1,
    }

    def __init__(self, port_code=None, date_range=None, **kwargs):
        """Init SouthKorea spider search filters.

        Args:
            port_code (str): three-digit code, see https://bit.ly/2JXbu3E for port name mapping
            date_range (str): formatted as "YYYYMMDD,YYYYMMDD",
                              defaults to "<14-days-prior>,<7-days-in-the-future>"

        """
        super().__init__(**kwargs)

        if not date_range:
            start = dt.datetime.strftime(dt.datetime.utcnow() - dt.timedelta(days=14), '%Y%m%d')
            end = dt.datetime.strftime(dt.datetime.utcnow() + dt.timedelta(days=7), '%Y%m%d')
            date_range = start + ',' + end
        else:
            if len(date_range.split(',')) != 2:
                raise InvalidCliRun('date_range', repr(date_range))

            start, end = date_range.split(',')
            if not try_apply(date_range.split(',')[0], int):
                raise InvalidCliRun('date_range(start)', repr(date_range.split(',')[0]))

            if not try_apply(date_range.split(',')[1], int):
                raise InvalidCliRun('date_range(end)', repr(date_range.split(',')[1]))

        # choose whether we'd like to limit data extraction to only a single port
        self.limit_port_code = str(port_code) if port_code else None
        # WARNING setting a large date_range may result in excessively long run times
        self.start_date = start
        self.end_date = end
        self.filters = kwargs

    def start_requests(self):
        """Request list of vessel lineup for specified port and date ranges.

        Yields:
            Request:

        """
        for port_code in normalize.PORT_MAPPING.keys():
            if self.limit_port_code and self.limit_port_code != port_code:
                self.logger.info("Skip data extraction for port_code=%s", port_code)
                continue

            form = {
                'bargeClsgn1': '',  # optional
                'bargeVsslNm': '',  # optional
                'clsgn': '',  # optional
                'currentPageNo': 1,
                'etryptTkoffDt': '',  # optional
                'ibobprtSe': '1',
                'prtAgCd': port_code,
                'prtAgNm': '',  # optional
                'recordCount': 1000000,  # reasonably large enough number for long date ranges
                'reqstSe': 'all',
                'srchBeginEtryndDt': self.start_date,
                'srchEndEtryndDt': self.end_date,
                'vsslInnb': '',  # optional
                'vsslNm': '',  # optional
            }
            form.update(self.filters)
            yield Request(
                url=self.lineup_url,
                callback=self.collect_responses,
                method='POST',
                headers={'content-type': 'application/json; charset=utf-8'},
                body=json.dumps({'dmaParam': form}),
            )

    @staticmethod
    def init_form_bol(**opts):
        """Init form data for POSTing.

        Args:
            opts (list[str]):

        Returns:
            str: json-formatted string

        """
        return json.dumps(
            {
                'dmaParam': {
                    'prtAgCd': opts.get('prtAgCd'),
                    'clsgn': opts.get('clsgn'),
                    'vsslNm': opts.get('vsslNm'),
                    'etryptYear': opts.get('etryptYear'),
                    'etryptCo': opts.get('etryptCo'),
                    'entrpsCd': '',  # optional
                    'entrpsNm': '',  # optional
                    'tkinTkoutSe': '',  # optional
                    'lnlMthCd': '',  # optional
                    'conTp': 'A',
                    'lnlEntrpsCd': '',  # optional
                    'currentPageNo': 1,
                    'recordCount': 5000,
                    'stcPopGb': '',  # optional
                    'vsslInnb': opts.get('vsslInnb'),
                    'unityFrghtUpdtOdr': '',  # optional
                }
            }
        )

    @staticmethod
    def init_form_bol_cargo(**opts):
        """Init form data for POSTing.

        Args:
            opts (list[str]):

        Returns:
            str: json-formatted string

        """
        return json.dumps(
            {
                'dmaParam': {
                    'prtAgCd': opts.get('prtAgCd'),
                    'clsgn': opts.get('clsgn'),
                    'etryptYear': opts.get('etryptYear'),
                    'etryptCo': opts.get('etryptCo'),
                    'entrpsCd': opts.get('entrpsCd'),
                    'tkinTkoutSe': opts.get('tkinTkoutSe'),
                    'lnlMthCd': '',  # optional
                    'blNo': opts.get('blNo'),
                    'unityFrghtUpdtOdr': 1,
                }
            }
        )

    @staticmethod
    def init_form_decl(**opts):
        """Init form data for POSTing.

        Args:
            opts (list[str]):

        Returns:
            str: json-formatted string

        """
        # goods declaration submission date is not indicated anywhere
        # we therefore need to widen the search range, to 6 days before the vessel arrival date
        lower_date = dt.datetime.strptime(opts['etryptDt'][:-4], '%Y%m%d') - dt.timedelta(days=6)
        if opts.get('tkOfftkoffDt'):  # may not be present always
            upper_date = dt.datetime.strptime(opts['tkoffDt'][:-4], '%Y%m%d')
        else:
            upper_date = dt.datetime.strptime(opts['etryptDt'][:-4], '%Y%m%d')

        return json.dumps(
            {
                'dmaParam': {
                    'clsgn': opts.get('clsgn'),
                    'currentPageNo': 1,
                    'entrpsCd': '',  # optional
                    'entrpsCdNm': '',  # optional
                    'fromReqstDt': lower_date.strftime('%Y%m%d'),
                    'prtAgCd': opts.get('prtAgCd'),
                    'recordCount': 5000,
                    'toReqstDt': upper_date.strftime('%Y%m%d'),
                }
            }
        )

    @staticmethod
    def init_form_decl_cargo(**opts):
        """Init form data for POSTing.

        Args:
            opts (list[str]):

        Returns:
            str: json-formatted string

        """
        return json.dumps(
            {
                'dmaDetailParam': {
                    'recordDngrCount': 1000,
                    'tkinSe': opts.get('tkinSe'),
                    'vsslInnb': opts.get('vsslInnb'),
                    'currentDngrPageNo': 1,
                    'satmntUpdtOdr': 1,
                    'recordCargCount': 5000,
                    'etryptCo': opts.get('etryptCo'),
                    'currentCargPageNo': 1,
                    'prtAgCd': opts.get('prtAgCd'),
                    'etryptYear': opts.get('etryptYear'),
                }
            }
        )

    def post(self, url, form, **kwargs):
        """Post form.

        Args:
            url (str): post endpoint
            form (str): json-deserialised string
            kwargs (dict[str, str]): additional meta to append to response

        Returns:
            scrapy.Request:

        """
        return Request(
            url=url,
            method='POST',
            headers={'content-type': 'application/json; charset=utf-8'},
            body=form,
            meta=kwargs,
        )

    @allow_inline_requests
    def collect_responses(self, response):
        """Collect all POST responses.

        Serialise json response.
        Transform into raw dict.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        # each vessel in the lineup has a number of BOLs or goods declaration associated with it
        length = len(json.loads(response.body)['dltInOutList'])
        for idx, raw_item in enumerate(json.loads(response.body)['dltInOutList']):
            raw_item['cargoes'] = []
            self.logger.info(
                'Port {} ({}/{}) : Vessel {} (IMO {}) (Callsign {}) (Type {})'.format(
                    raw_item['prtAgCd'],
                    idx + 1,
                    length,
                    raw_item['vsslNm'],
                    raw_item['vsslInnb'],
                    raw_item['clsgn'],
                    raw_item['vsslKindNm'],
                )
            )

            # NOTE technically this should be in `normalize.py`,
            # but putting this here cuts down on run times by more than 80 %
            if raw_item['vsslKindNm'] not in normalize.IRRELEVANT_VESSEL_TYPES:

                # each BOL contains product/volume/movement/charterers data
                bol_numbers = yield self.post(
                    url=self.bol_url, form=self.init_form_bol(**raw_item), **raw_item
                )
                for bol_number in json.loads(bol_numbers.body)['dltList1']:
                    raw_item.update(bol_number)
                    # get individual cargo data and append to list of cargoes
                    bol_cargo = yield self.post(
                        url=self.cargo_url, form=self.init_form_bol_cargo(**raw_item), **raw_item
                    )
                    raw_cargo = json.loads(bol_cargo.body)['dmaRstMap1']
                    # there are errors on the website sometimes where it does not return any cargo
                    if not raw_cargo:
                        continue

                    # Sometimes, BOL will show "TANK" as the product, which is incorrect
                    # this is a one-off special case where we do not want this product
                    if any(tank in raw_cargo['frghtNm'] for tank in ['TANK', 'TNK']):
                        continue

                    raw_item['cargoes'].append(raw_cargo)

                # sometimes, BOL will not be present (i.e. 'cargoes' will be empty)
                if not raw_item['cargoes']:
                    # if so, search for goods declaration sheets related to that vessel
                    # declaration contains a key needed for obtaining the dangerous goods data
                    declarations = yield self.post(
                        url=self.danger_goods_url, form=self.init_form_decl(**raw_item), **raw_item
                    )
                    declarations = json.loads(declarations.body)['dtlDgst1010List']
                    for idx, decl in enumerate(declarations):
                        # possible to have more than one goods declaration form for one port call
                        # if so, take the latest, most up-to-date goods declaration
                        if idx + 1 != len(declarations):
                            continue

                        raw_item.update(decl)
                        # get list of all dangerous cargo onboard from declaration sheet
                        danger_cargoes = yield self.post(
                            url=self.cargo_alt_url,
                            form=self.init_form_decl_cargo(**raw_item),
                            **raw_item,
                        )

                        danger_cargo = json.loads(danger_cargoes.body)['dtlDgst1010Map']
                        raw_item['cargoes'].append(danger_cargo)

                # contextualise raw item with metadata
                raw_item.update(provider_name=self.provider)
                yield normalize.process_item(raw_item)
