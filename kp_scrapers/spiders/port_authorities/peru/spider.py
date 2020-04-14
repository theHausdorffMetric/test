"""Peruvian customs BOLs spider

This spider scrapes data from the Peruvian national customs website. It provides information
about bills of ladding transiting through Peru. This contains valuable information about
vessels' cargo such as the destination port, the unloading date, the cargo weight, and also
and above all information about the product transported and its grade.

Website URL: http://www.aduanet.gob.pe/aduanas/informao/HRMCFLlega.htm

The website has some limitations for data querying that we had to bypass:
    - it limits the information range of date you can query to 30 days
    - the relevant information of BOLs is protected by a simple captcha

"""

import re
from urllib.parse import urlencode

from furl import furl  # used to ship faster, TODO: remove and use urllib.parse instead
from scrapy import Request, Spider

from kp_scrapers.lib.captcha import solve_captcha
from kp_scrapers.lib.parser import may_strip, row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.peru import normalize, utils


# Default 'Accept' header
ACCEPT = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'

# Default values for main page POST request
TERMINAL_CODE = '0000-TODOS'
CARGO_TYPE = 118


class PeruCustomsSpider(PortAuthoritySpider, Spider):
    name = 'Peru'
    provider = 'SUNAT'
    version = '2.0.2'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        # endpoint for querying vessel schedules
        'http://www.aduanet.gob.pe/servlet/CRManFLlega',
        # endpoint for retrieving captcha challenge
        'http://www.aduanet.gob.pe/servlet/captcha?accion=image',
    ]

    spider_settings = {
        # sequential requests necessary for captcha to succeed
        'CONCURRENT_REQUESTS': 1
    }

    @property
    def headers(self):
        return {
            'Accept': ACCEPT,
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
        }

    def __init__(self, start_date=None, end_date=None):
        # The range of dates defined must not exceed 30 days as a limitation set by the website. By
        # default, we scrape 7 days of data so that the scraper is fast enough to run on production
        self.start_date = start_date if start_date else utils.get_datenow_with_offset(days=-7)
        self.end_date = end_date if end_date else utils.get_datenow_with_offset()

    def start_requests(self):
        """Parse the website main page and submit form data to query a range of dates < 30 days"""
        formdata = urlencode(
            {
                'mAdua': CARGO_TYPE,
                'CMc2_Fecha1': self.start_date,
                'CMc2_Fecha2': self.end_date,
                'CMc2_Terminal': TERMINAL_CODE,
                'CMc2_DNave': '',
                'TipM': 'mc',
            }
        )

        # submit the main page form with the range of dates to query prepared above
        yield Request(
            url=self.start_urls[0],
            method='POST',
            headers=self.headers,
            body=formdata,
            callback=self.parse_list_of_manifests,
        )

    def parse_list_of_manifests(self, response):
        # retrieve the list of manifests urls and forge the next request to process
        manifests_urls = [
            re.sub(r';.*(?=\?)', '', response.urljoin(manifest))
            for manifest in response.xpath('//tr//td/a/@href').extract()
        ]

        for manifest in manifests_urls:
            # captcha is required to enter the manifest page
            yield Request(url=manifest, meta={'manifest': manifest}, callback=self.captcha_request)

    def captcha_request(self, response):
        """Forge request to get captcha image"""
        return Request(
            url=self.start_urls[1],
            headers=self.headers,
            meta={'manifest': response.meta['manifest']},
            callback=self.solve_captcha_challenge,
            # disables filtering to allow requesting multiple times
            dont_filter=True,
        )

    def solve_captcha_challenge(self, response):
        """Process captcha image and submit computed solution"""
        url_partials = furl(response.meta.get('manifest'))
        form_data = {
            'codigo': '',
            'sessionCaptcha': 'null',
            'CG_cadu': url_partials.args['CG_cadu'],
            'CMc1_Anno': url_partials.args['CMc1_Anno'],
            'CMc1_Terminal': url_partials.args['CMc1_Terminal'],
            'CMc1_Numero': url_partials.args['CMc1_Numero'],
            'TipM': url_partials.args['TipM'],
            'viat': url_partials.args['viat'],
            'strMenu': '',
            'xadu': '',
            'strDepositoIn': '',
            'strDeposito': '',
            'strEmprTransporte': '',
            'strEmprTransporteIn': '',
            'strEmpresaMensa': '',
            'strEmpTransTerrestre': '',
        }

        # solve captcha and format solution
        _captcha = may_strip(solve_captcha(response).upper()).replace(' ', '')
        self.logger.debug(f'Captcha attempt: {_captcha}')
        captcha_solution = f'codigo={_captcha}&'

        # submit captcha solution with data prepared above
        return Request(
            url='http://www.aduanet.gob.pe/servlet/CRManManif',
            method='POST',
            body=f'{captcha_solution}, {urlencode(form_data)}',
            headers=self.headers,
            meta={'manifest': response.meta['manifest']},
            callback=self.is_captcha_cracked,
        )

    def is_captcha_cracked(self, response):
        if 'la imagen no coincide' in response.text:
            self.logger.debug(f'Captcha attempt failed, attempting again ...')
            return self.captcha_request(response)
        else:
            self.logger.debug(f'Captcha attempt succeeded, proceeding ...')
            return self.parse_manifest_page(response)

    def parse_manifest_page(self, response):
        """Parse manifest detail page response and extract desired information."""
        # no cargo info present, discard
        if 'no tiene conocimientos' in response.text.lower():
            return

        pc_header = [
            may_strip(head.extract())
            for head in response.xpath('//body/table[@width="80%"]//tr//td//b/text()')
        ]
        portcall = response.xpath('//body/table[@width="80%"]//tr//td/text()').extract()
        portcall = dict(zip(pc_header, portcall))
        portcall.update(
            cargoes=[],
            port_names=set(),
            provider_name=self.provider,
            # NOTE we need to introspect url per `raw_cargo` to append data to `cargoes`
            raw_cargoes=[],
        )

        # NOTE first element of `cargoes` is the header
        cargoes = response.xpath('//body/table[@width="100%"]//tr')
        header = [may_strip(head) for head in cargoes[0].xpath('.//td//text()').extract()]
        cargoes = cargoes[1:]

        for raw_row in cargoes:
            raw_cargo = row_to_dict(
                raw_row,
                header,
                cargo_url=response.urljoin(raw_row.xpath('.//a/@href').extract()[-1]),
            )

            # reported_date is based on manifest publish date, take earliest one for consistency
            if not portcall.get('reported_date'):
                _reported_date = raw_cargo.get('Fecha de Transmisión') or raw_cargo.get(
                    'Fecha de Transmisi�n'
                )
                portcall.update(reported_date=_reported_date)

            # easy optimization; don't care about cargoes in-transit and not bound for Peru
            # destinations are given as UN/LOCODEs
            if not raw_cargo['Puerto Destino'].startswith('PE'):
                continue

            portcall['raw_cargoes'].append(raw_cargo)

        return self.extract_cargo_data(portcall)

    def extract_cargo_data(self, portcall):
        # raw cargoes remaining that we need to get product details with
        if portcall['raw_cargoes']:
            raw_cargo = portcall['raw_cargoes'].pop(0)
            # find all potential Peru-bound portcalls within a manifest
            # there may be more than one per manifest
            portcall['port_names'].add(raw_cargo['Puerto Destino'])

            return Request(
                # this url is always present, else resource would likely have changed
                # crash spider if so
                url=raw_cargo['cargo_url'],
                meta={'raw_item': portcall},
                callback=self.parse_cargo_pages,
            )

        # all product details obtained, proceed to normalize
        return normalize.process_item(portcall)

    def parse_cargo_pages(self, response):
        portcall = response.meta['raw_item']

        # easy optimization; don't care about container vessels
        if 'contenedore' in response.xpath('body').extract_first().lower():
            # break if one container cargo is present; no need to introspect the other cargoes
            self.logger.info(
                f"Vessel {portcall.get('Matrícula de la Nave')} is a container ship, discarding ..."
            )
            return

        header = [
            may_strip(head.extract())
            for head in response.xpath('//body/table[@width="100%"]//tr[1]//b/text()')
            if may_strip(head.extract())
        ]
        products = response.xpath('//body/table[@width="100%"]//tr[position()>1]')

        for product in products:
            product = row_to_dict(product, header)
            portcall['cargoes'].append(product)

        return self.extract_cargo_data(portcall)
