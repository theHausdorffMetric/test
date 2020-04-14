import csv
import datetime as dt
import json
import logging
import re
from urllib.parse import urlencode

from scrapy import Request

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.contracts import ContractSpider
from kp_scrapers.spiders.contracts.bill_of_lading import api, normalize


logger = logging.getLogger(__name__)


# ImportGenius can add Bill of Lading (bol) at worst 6 days later
DEFAULT_BACKLOG = 7

# NOTE if you update DEFAULT_QUERY, please also update DEFAULT_NOT_TERMS
DEFAULT_QUERY = """
    AND crude, OR API gravity, OR crude oil, OR alkylate, OR avgas, OR barr*, OR bbl*, OR blend,
    OR cbob, OR condensate, OR crude api, OR diesel, OR etbe, OR fuel, OR gas oil, OR gasoil,
    OR gasoline, OR gtab, OR heating oil, OR jet, OR mogas, OR mtbe, OR naphta, OR naphtha,
    OR pbob, OR rbob, OR reformate, OR ulsd, OR unleaded, OR mgo, OR v.g.o, OR vgo, OR hsvgo,
    OR lsfo, OR lsvgo, OR kerosene, OR toluene, OR coconut oil, OR ammon*, OR sulphuric acid,
    OR acetone, OR canola oil, OR palm oil, OR methanol, OR b100, OR molasse, OR ethanol,
    OR olive oil, OR benzene, NOT alcohol*, NOT alm, NOT alu*, NOT ananas, NOT argentine sme,
    NOT aspha*, NOT avocado, NOT bag, NOT barite, NOT beverag*, NOT butane, NOT coffee,
    NOT cornmint, NOT cotton, NOT credit, NOT distilled, NOT equipment, NOT fat, NOT film,
    NOT fish, NOT food, NOT galvanized, NOT glycerine, NOT ground*, NOT guatemala, NOT gum*,
    NOT household, NOT iron, NOT octanol, NOT ore, NOT pack*, NOT peach, NOT peanut, NOT peruvian,
    NOT pineappl*, NOT pneumatic, NOT propane, NOT rains, NOT rapeseed, NOT research, NOT roofing,
    NOT safflower, NOT sesame, NOT slops, NOT soap, NOT solubl*, NOT washout
"""

# Default `not_terms` from default query.
# If we input a custom query, `not_terms` are recalculated from the query and passed in the items.
DEFAULT_NOT_TERMS = [
    'accessories',
    'accumulator',
    'aircraft',
    'alm',
    'aluminium',
    'alumminium',
    'analysis',
    'ananas',
    'animal',
    'aroma',
    'articulated',
    'aspargus',
    'assembly',
    'automotive',
    'avocado',
    'baby',
    'baffle',
    'bag',
    'bale',
    'bamboo',
    'barite',
    'barras',
    'barrette',
    'barricade',
    'barrier',
    'batman',
    'batteries',
    'beef',
    'beer',
    'beeswax',
    'blind',
    'body',
    'boiler',
    'bone',
    'book',
    'books',
    'borja',
    'bourbon',
    'bowl',
    'boxes',
    'brewing',
    'brush',
    'bubble',
    'burner',
    'cabernet',
    'cabinet',
    'cable',
    'california',
    'candle',
    'candy',
    'canola',
    'canopied',
    'car',
    'carton',
    'cartons',
    'cashew',
    'caskmates',
    'casks',
    'cauliflower',
    'cavit',
    'centrifuge',
    'ceramic',
    'cereal',
    'chafing',
    'chain',
    'chair',
    'charcoal',
    'cheese',
    'chevrolet',
    'chili',
    'chivas',
    'chocolate',
    'chrome',
    'clothing',
    'coat',
    'coax',
    'cocoa',
    'coffee',
    'components',
    'composite',
    'container',
    'control',
    'conversion',
    'cook',
    'cooker',
    'cooler',
    'copper',
    'corona',
    'cotton',
    'cover',
    'crafts',
    'crane',
    'crated',
    'crates',
    'credit',
    'cube',
    'cushion',
    'cushions',
    'cylinder',
    'decorative',
    'deodorant',
    'distilled',
    'door',
    'drums',
    'educational',
    'effect',
    'electric',
    'electrical',
    'empty',
    'engine',
    'engines',
    'equipment',
    'fabrics',
    'fajita',
    'fat',
    'film',
    'filter',
    'fish',
    'flammable',
    'flange',
    'food',
    'footwear',
    'fork',
    'fountain',
    'fragrance',
    'frozen',
    'fruit',
    'fry',
    'furniture',
    'galvanizadas',
    'galvanizados',
    'galvanized',
    'garment',
    'gel',
    'generating',
    'generatoe',
    'generator',
    'generatos',
    'gherkin',
    'glass',
    'glove',
    'goods',
    'grapefruit',
    'grill',
    'ground',
    'groundnut',
    'guard',
    'guatemala',
    'hair',
    'hammer',
    'head',
    'headlamp',
    'heineken',
    'helium',
    'honda',
    'honey',
    'honour',
    'hornitos',
    'household',
    'hydrat',
    'hydrocarbon',
    'hygiene',
    'injection',
    'injector',
    'ink',
    'insulation',
    'iron',
    'items',
    'jam',
    'jameson',
    'jar',
    'jeep',
    'jewelry',
    'joint',
    'juice',
    'keyless',
    'kit',
    'kitchen',
    'kosher',
    'krombacher',
    'lader',
    'ladies',
    'launcher',
    'leather',
    'lemon',
    'level',
    'linen',
    'litemax',
    'lng',
    'looms',
    'lssr',
    'lug',
    'machin',
    'machine',
    'malbec',
    'malt',
    'manufacture',
    'marker',
    'men',
    'mens',
    'metal',
    'micro',
    'milk',
    'mirror',
    'model',
    'module',
    'monopod',
    'monster',
    'multirgai',
    'mushroom',
    'mustard',
    'napkin',
    'natural gas',
    'neck',
    'needle',
    'neon',
    'nissan',
    'nozle',
    'nut',
    'nylon',
    'oak',
    'office',
    'olive',
    'orange',
    'ore',
    'organizer',
    'outdoor',
    'oxidation',
    'package',
    'packing',
    'paint',
    'pallet',
    'pallets',
    'parks',
    'part',
    'parts',
    'peach',
    'peanut',
    'pen',
    'pepper',
    'personnal',
    'peruvian',
    'pick',
    'piece',
    'pieces',
    'pilates',
    'pillar',
    'pillow',
    'pineapple',
    'pipe',
    'piping',
    'piston',
    'pit',
    'plantain',
    'plantains',
    'plastic',
    'pneumatic',
    'polyols',
    'poo',
    'potato',
    'propane',
    'pump',
    'pumps',
    'racket',
    'radiator',
    'rail',
    'rain',
    'rains',
    'rare',
    'rear',
    'refrigerante',
    'refrigerator',
    'regenerated',
    'remote',
    'reprocessing',
    'research',
    'resistance',
    'rice',
    'roof',
    'roofing',
    'rope',
    'rotary',
    'rover',
    'rum',
    'sacks',
    'salt',
    'sample',
    'sand',
    'scarf',
    'screen',
    'screw',
    'screws',
    'seed',
    'sensor',
    'sesame',
    'shell',
    'shirt',
    'shoe',
    'shoes',
    'silicon',
    'slops',
    'sludge',
    'smoke',
    'soap',
    'sock',
    'socks',
    'sofa',
    'solenoid',
    'spare',
    'spares',
    'spice',
    'square',
    'stator',
    'steel',
    'sticky',
    'stone',
    'stones',
    'straps',
    'strapwinch',
    'style',
    'subaru',
    'sugar',
    'surfboard',
    'suzuki',
    'switch',
    'syst',
    'system',
    'tank',
    'tea',
    'telescopia',
    'tequila',
    'thrustwasher',
    'tie',
    'tires',
    'titan',
    'tools',
    'towel',
    'towing',
    'toy',
    'toys',
    'trader',
    'truck',
    'tube',
    'tubes',
    'turbojet',
    'unpacked',
    'used',
    'valve',
    'vehicle',
    'viscose',
    'volvo',
    'wagon',
    'wall',
    'walnut',
    'washout',
    'water',
    'waxe',
    'wedge',
    'whiskey',
    'whisky',
    'wine',
    'women',
    'womens',
    'wood',
    'wool',
    'woven',
    'zinc',
]


def parse_response_info(jsresp):
    """Parse response info.

    Args:
        jsresp (TYPE): Description

    Returns:
        TYPE: Description
    """
    response = jsresp.get('response')
    return ''.join(response.xpath('//text()').extract())


class ImportGeniusBolSpider(ContractSpider):
    """Bill of lading (BOL) spider of data from Import Genius (IG).

    We can extract data from html or csv.
    Default mode is html. However Import Genius is hiding some data on html.
    Use csv mode on prod. We can only extract a certain number of csv lines by month, be cautious!

    csv mode: login -> get_mapping -> search -> generate_csv ->
              get_status while percent != 100 -> download
    html mode: login -> get_mapping -> search -> generate_html

    search criteria:
        Search criteria must start with `AND`.
        Search criteria must not exceeds the 100 limit.
        You can use an asterisk * in shearch criteria.
        An asterisk must be preceded by at leat one letter.
            Example: search for bat*, will include results which begins with bat such as:
            bats, battery, bathroom, etc."

    query:
        IG allow to pass not terms in query. However they are not proprely filtered.
        Moreover as stated above, query can contains at most 100 terms.
            Example: 'AND curde oil NOT coconut' will match crude coconut oil
        Thus, we filter them with a normalisation function.

    not_terms:
        Default not terms corresponds to not terms from default query.
        You can pass custom not terms to the spider as a string of coma separated terms.
            Example: scrapy -a not_terms='coconut,ketchup'
        If a custom query is passed as input, not terms are recomputed  and passed into the items.


    Attributes:
        backlog (int): number of days to fetch backwards (default 2)
        mode (str): Any value will pass on html (default 'csv')
        query (str): custom query to use with import genius
        not_terms (str): list of coma separated terms to filter
        start_date (str): Format 'YYYY-mm-dd'

    """

    name = 'BillOfLading'
    version = '1.2.0'
    provider = 'BillOfLading'  # TODO should be ImportGenius provider actually
    produces = [DataTypes.BillOfLading]

    spider_settings = {
        # enforce due diligence checks on IP address location
        'GEOLOCATION_ENABLED': True,
        'GEOLOCATION_STRICT': True,
        'GEOLOCATION_CITY': 'London, GB',
        # be kind to website, so that we don't get our paid account banned
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_DELAY': 10,
    }

    mode = None
    sid = None  # Query id
    pid = None  # File id

    def __init__(
        self,
        user,
        password,
        query=None,
        not_terms=None,
        backlog=None,
        start_date=None,
        mode='html',
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        # ImportGenius account details
        self.user = user
        self.password = password

        self.backlog = int(backlog or DEFAULT_BACKLOG)

        if start_date:
            self.start_date = dt.datetime.strptime(start_date, '%Y-%m-%d')
        else:
            # target somewhere in the US
            us_lag = 6
            self.start_date = dt.datetime.utcnow() - dt.timedelta(hours=us_lag)

        self.mode = mode.lower()
        assert self.mode in ('csv', 'html')

        self.query = query or DEFAULT_QUERY

        if not_terms:
            self.not_terms = [e.strip().lower() for e in not_terms.split(',')]
        else:
            self.not_terms = DEFAULT_NOT_TERMS

    def start_requests(self):
        yield api.login(username=self.user, password=self.password, callback=self.on_logged_in)

    def on_logged_in(self, response):
        yield api.get_mapping(callback=self.on_mapping)

    def on_mapping(self, response):
        """Get columns mapping when using html mode
        """
        mapping = json.loads(response.body)
        columns = [i['display'] for i in mapping['fields']]
        return self.search(columns)

    def search(self, columns):
        # Default parameters, we only specify from and to fields
        parameters = [
            ('page', 1),
            ('rp', 15),
            ('from', (self.start_date - dt.timedelta(days=self.backlog)).strftime('%m/%d/%Y')),
            ('to', self.start_date.strftime('%m/%d/%Y')),
            ('from_ex', (self.start_date - dt.timedelta(days=self.backlog)).strftime('%m/%d/%Y')),
            ('to_ex', self.start_date.strftime('%m/%d/%Y')),
            ('query', ''),
            ('qtype', ''),
            ('action', 'main/maketable'),
            ('sidforcontactcompanies', ''),
            ('exclude', ''),
        ]
        parameters = api.build_query(self.query, parameters)

        # Generate csv or scroll website depending on the spider mode
        if self.mode == 'csv':
            callback = self.generate_csv
            meta = None
        elif self.mode == 'html':
            callback = self.generate_html
            meta = {'page': 1, 'columns': columns}

        # TODO factorise and move to `api.py`
        yield Request(
            url=api.URL_SEARCH,
            method='POST',
            headers=api.HEADERS,
            body=parameters,
            callback=callback,
            meta=meta,
            errback=api._request_failed,
        )

    def generate_html(self, response):
        jsresp = json.loads(response.body)

        if len(jsresp['rows']) == 0:
            self.logger.debug("Reached end of pages: %s", jsresp.get('response'))
            return

        # Got Results
        for belt in jsresp['rows']:
            raw_item = {k.upper(): v for k, v in zip(response.meta['columns'], belt['cell'])}
            # contextualise with meta data
            raw_item.update(provider_name=self.provider)

            # discard if BL describes a container vessel (not relevant to Kpler business)
            # "NC" is shorthand for "no container"
            if may_strip(raw_item.get('CONTAINER NUMBER')) != 'NC':
                self.logger.debug('Irrelevant bill of lading:\n%s', raw_item)
                continue

            # get detailed product description
            if raw_item.get('VIEW'):
                cargo_url = self._get_cargo_url(raw_item['VIEW'])
                yield Request(url=cargo_url, callback=self.parse_html, meta={'raw_item': raw_item})

        # continue scrolling as long as we have results
        # TODO factorise and move to `api.py`
        yield Request(
            url=api.URL_SEARCH,
            method='POST',
            headers=api.HEADERS,
            body=urlencode(
                [
                    ('page', response.meta['page']),
                    ('rp', 100),
                    ('sortname', 'actdate'),
                    ('sortorder', 'desc'),
                    ('query', ''),
                    ('qtype', ''),
                    ('sid', jsresp['sid']),
                ]
            ),
            callback=self.generate_html,
            meta={'page': response.meta['page'] + 1, 'columns': response.meta['columns']},
        )

    def parse_html(self, response):
        """Parse HTML modal containing detailed product description, including volumes."""
        _xpath = '//div[contains(text(), "PRODUCT")]/following-sibling::node()[2]//td[2]/text()'
        product_desc = response.xpath(_xpath).extract_first()

        # override with detailed product description
        raw_item = response.meta['raw_item']
        raw_item['PRODUCT DESCRIPTION'] = product_desc
        yield normalize.process_item(raw_item, self.not_terms)

    def _get_cargo_url(self, url):
        """Retrieve a url from inside an <a> html tag."""
        match = re.search(r'href=\"(\S+)\"', url)
        if not match:
            self.logger.error("Unknown bill of lading cargo url: %s", url)
            return

        return match.group(1)

    def generate_csv(self, response):
        jsresp = json.loads(response.body)
        self.logger.debug(parse_response_info(jsresp))
        self.sid = str(jsresp.get('sid'))

        # Default parameters, we need to specify from, to, and sid fields
        # Parameters have been reverse-engeenired from importgenius
        # Order is important
        parameters = [
            ('action', 'export/generate'),
            ('from', (self.start_date - dt.timedelta(days=self.backlog)).strftime('%m/%d/%Y')),
            ('from_ex', (self.start_date - dt.timedelta(days=self.backlog)).strftime('%m/%d/%Y')),
            ('to', self.start_date.strftime('%m/%d/%Y')),
            ('to_ex', self.start_date.strftime('%m/%d/%Y')),
            ('export_fields', ','.join("'{}'".format(e) for e in api.CSV_EXPORT_FIELD)),
            ('format', 'csv'),
            ('fname', ''),
            ('rec', 1),
            ('nfrom', 1),
            ('nto', 100000),
            ('notify_email', 'toghrul.aliyev@edu.escpeurope.eu'),
            ('sid', self.sid),
            ('action', 'main/maketable'),
            ('sidforcontactcompanies', self.sid),
            ('exclude', 1),
        ]
        parameters = api.build_query(self.query, parameters)

        # TODO factorise and move to `api.py`
        yield Request(
            url=api.URL_GENERATE_CSV,
            method='POST',
            headers=api.HEADERS,
            body=parameters,
            callback=self.get_status,
        )

    def get_status(self, response):
        jsresp = json.loads(response.body)
        self.logger.debug(parse_response_info(jsresp))
        self.pid = jsresp.get('pid')

        # TODO factorise and move to `api.py`
        yield Request(url=api.URL_STATUS.format(pid=self.pid), callback=self.on_status_result)

    def on_status_result(self, response):
        """Wait for csv to be generated by the website
        """
        jsresp = json.loads(response.body)
        percent = jsresp.get('percent')
        if percent == '100':
            # Download csv when ready
            # TODO factorise and move to `api.py`
            yield Request(
                url=api.URL_DOWNLOAD_CSV_BASE.format(sid=self.sid, pid=self.pid),
                callback=self.parse_csv,
            )
        else:
            # Or check for status
            # TODO factorise and move to `api.py`
            yield Request(
                url=api.URL_STATUS.format(pid=self.pid),
                callback=self.on_status_result,
                dont_filter=True,
            )

    def parse_csv(self, response):
        reader = csv.DictReader(response.body.splitlines(), delimiter=',')
        for row in reader:
            raw_item = {k.upper(): v for k, v in row.items()}
            # contextualise with meta data
            raw_item.update(provider_name=self.provider)
            yield normalize.process_item(raw_item, self.not_terms)
