import datetime as dt
import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.excel import GenericExcelExtractor
from kp_scrapers.lib.parser import may_remove_substring, may_strip, try_apply
from kp_scrapers.lib.utils import scale_to_thousand
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


PORT_MAPPING = {
    'BEDI': 'BEDI',
    'BGB': 'BUDGE-BUDGE',
    'BHOGAT': 'BHOGAT',
    'BORL': 'BORL',
    'CHE': 'CHENNAI',
    'CUDL': 'CUDDALORE',
    'DHBL': 'DABHOL',
    'DHIL': 'DAHEJ',
    'DHJ-G': 'DAHEJ',
    'DHJPL': 'DAHEJ',
    'DNL': 'KANDLA',
    'GOA': 'GOA',
    'GSFC': 'GSFC',
    'GVRM': 'GANGAVARAM',
    'HAZAN': 'HAZIRA',
    'HAZSB': 'HAZIRA',
    'HAZSH': 'HAZIRA',
    'HLD': 'HALDIA',
    'HZADN': 'HAZIRA',
    'INNML': 'NEW MANGALORE',
    'JNPT': 'JNPT',
    'KAK': 'KAKINADA',
    'KAM': 'KAMARAJAR',
    'KDL': 'KANDLA',
    'KOCHI': 'KOCHI',
    'KOCSB': 'KOCHI',
    'KOL': 'KOLKATA',
    'KRISH': 'KRISHNAPATNAM',
    'KRKL': 'KARAIKAL',
    'KWR': 'KARWAR',
    'MBANC': 'MUMBAI',
    'MBID': 'MUMBAI',
    'MBJD': 'MUMBAI',
    'MBLPO': 'MUMBAI',
    'MBPP': 'MUMBAI',
    'MGL': 'MANGALORE',
    'MGSPM': 'MANGALORE',
    'MUN': 'MUNDRA',
    'MUN-H': 'MUNDRA',
    'MUN-I': 'MUNDRA',
    'MUN-L': 'MUNDRA',
    'NAG': 'NAGAPATTINAM',
    'NAV': 'NAVLAKHI',
    'OKHA': 'OKHA',
    'PBDR': 'PORBANDAR',
    'PBL': 'PORT BLAIR',
    'PDP': 'PARADIP',
    'PDP-S': 'PARADIP',
    'PIPAV': 'PIPAVAV',
    'RAV': 'RAVVA',
    'RTGR': 'RATNAGIRI',
    'SIKKA': 'SIKKA',
    'TIR': 'THIRUKADAIYUR',
    'TTC': 'TUTICORIN',
    'VDN-I': 'VADINAR',
    'VDN-L': 'VADINAR',
    'VDNEFJ': 'VADINAR',
    'VDNEJ': 'VADINAR',
    'VDNES': 'VADINAR',
    'VZG': 'VISAKHAPATNAM',
    'VZG-L': 'VISAKHAPATNAM',
    'VZG-S': 'VISAKHAPATNAM',
}

# Map inconsistencies and spelling errors
SPELLING_MAPPING = {'P/ACID': 'P.ACID', 'A/ACID': 'A.ACID', 'G/CARGO': 'G.CARGO'}

MOVEMENT_MAPPING = {'D': 'discharge', 'L': 'load'}

# Discard whole item if they have this
CARGO_BLACKLIST = [
    'GNL.CGO',
    'CEMENT CLINKER',
    'CONT',
    'CARS',
    'G.CARGO',
    'PROJ',
    'PROJECT',
    'MACHINERY CARGO',
    'NA',
    'BOXES',
    'BUNKERING',
    'IMPORTATION',
    'PROJECT CARGO',
    'WOODEN CASES EMBARK' 'CR COILS+PROJECT',
    'VEHICLES+MACHINERY',
    'RORO UNITS PKGS',
    'UNITS',
    'UNITS CARS PROJECT',
    'CBM PROJ CARGO',
    'AGGREGATES',
    'REPAIRS',
    'SERVICE',
]


def extract_reported_date(name, regex, email_date):
    match = re.search(regex, name)
    return match.group(0) if match else email_date


@validate_item(PortCall, normalize=True, strict=False)
def parse_expected_vessels(row, reported_date, current_port, provider):
    if not MOVEMENT_MAPPING.get(row['e_movement']) or not may_strip(row['e_eta']):
        return

    for product, quantity in zip(*parse_product(map_spelling(row['e_cargo']), row['e_qty'])):
        if product == 'LPG':
            for p in ['Butane', 'Propane']:
                yield {
                    'reported_date': to_isoformat(reported_date),
                    'eta': normalize_date(reported_date, row['e_eta']),
                    'port_name': PORT_MAPPING.get(current_port, current_port),
                    'provider_name': provider,
                    'cargo': {
                        'product': p,
                        'movement': MOVEMENT_MAPPING.get(row['e_movement']),
                        'volume': try_apply(quantity, int, lambda x: x // 2, str),
                        'volume_unit': Unit.tons,
                    },
                    'vessel': {'name': row['e_vessel']},
                }
        else:
            yield {
                'reported_date': to_isoformat(reported_date),
                'eta': normalize_date(reported_date, row['e_eta']),
                'port_name': PORT_MAPPING.get(current_port, current_port),
                'provider_name': provider,
                'cargo': {
                    'product': product,
                    'movement': MOVEMENT_MAPPING.get(row['e_movement']),
                    'volume': try_apply(quantity, int, str),
                    'volume_unit': Unit.tons,
                },
                'vessel': {'name': row['e_vessel']},
            }


@validate_item(PortCall, normalize=True, strict=False)
def parse_vessel_movement(row, reported_date, current_port, provider):
    if not MOVEMENT_MAPPING.get(row['m_movement']):
        return

    for product, quantity in zip(*parse_product(map_spelling(row['m_cargo']), row['m_qty'])):
        if product == 'LPG':
            for p in ['Butane', 'Propane']:
                yield {
                    'reported_date': to_isoformat(reported_date),
                    'arrival': normalize_date(reported_date, row['m_arrived'])
                    if may_strip(row['m_arrived'])
                    else None,
                    'berthed': normalize_date(reported_date, row['m_berthed'])
                    if may_strip(row['m_berthed'])
                    else None,
                    'departure': normalize_date(reported_date, row['m_sailed'])
                    if may_strip(row['m_sailed'])
                    else None,
                    'port_name': PORT_MAPPING.get(current_port, current_port),
                    'provider_name': provider,
                    'cargo': {
                        'product': p,
                        'movement': MOVEMENT_MAPPING.get(row['m_movement']),
                        'volume': try_apply(quantity, int, lambda x: x // 2, str),
                        'volume_unit': Unit.tons,
                    },
                    'vessel': {'name': row['m_vessel']},
                }
        else:
            yield {
                'reported_date': to_isoformat(reported_date),
                'arrival': normalize_date(reported_date, row['m_arrived'])
                if may_strip(row['m_arrived'])
                else None,  # noqa
                'berthed': normalize_date(reported_date, row['m_berthed'])
                if may_strip(row['m_berthed'])
                else None,  # noqa
                'departure': normalize_date(reported_date, row['m_sailed'])
                if may_strip(row['m_sailed'])
                else None,  # noqa
                'port_name': PORT_MAPPING.get(current_port, current_port),
                'provider_name': provider,
                'cargo': {
                    'product': product,
                    'movement': MOVEMENT_MAPPING.get(row['m_movement']),
                    'volume': try_apply(quantity, int, str),
                    'volume_unit': Unit.tons,
                },
                'vessel': {'name': row['m_vessel']},
            }


def parse_product(product_str, qty_str):
    """Parse product string into 1 or more products and corresponding quantities

    Assign normally if a qty is given for each product.
    There are cases where there are more products than quantities,
    we divide that qty by the number of products.
    (Does not work for rare case: k products, n quantities, where k > n and n > 1)

    Examples:
        >>> parse_product('SM/ HAVY AROMATICS', '2/1')
        (['SM', 'HAVY AROMATICS'], [2000.0, 1000.0])
        >>> parse_product('PX/MEG', '40')
        (['PX', 'MEG'], [20000.0, 20000.0])
        >>> parse_product('TIMBER', '1941 LOGS/42202 CBM')
        (['TIMBER'], [None])
        >>> parse_product('CARS', '50')
        ([], [])

    """
    products = [may_strip(product) for product in product_str.split('/')]
    quantities = [
        scale_to_thousand(may_strip(may_remove_substring(quantity, ['\'', ';'])))
        for quantity in qty_str.split('/')
    ]
    if len(products) != len(quantities):
        if None in quantities:
            quantities = [None for _ in enumerate(products)]
        else:
            quantities = [quantities[0] / len(products)] * len(products)

    # filter out unwanted vessels
    for prod in products:
        if any(prod == blist for blist in CARGO_BLACKLIST):
            return [], []

    return products, quantities


def map_spelling(product):
    """Map product with spelling error to a consistent product name
    Example:
        >>> map_spelling('A/ACID')
        'A.ACID'
        >>> map_spelling('A ACID')
        'A ACID'

    """
    return SPELLING_MAPPING[product] if product in SPELLING_MAPPING else product


def normalize_date(raw_reported_date, row_date):
    """Check if date makes sense
    Args:
        raw_reported_date (str): date in 'dd.MM.yyyy' format
        row_date (str): date in 'dd-MM' format
    Example:
        >>> normalize_date('02.01.2019', '29-12')
        '2018-12-29T00:00:00'
        >>> normalize_date('02.01.2019', '02-01')
        '2019-01-02T00:00:00'
        >>> normalize_date('28.12.2018', '02-01')
        '2019-01-02T00:00:00'
        >>> normalize_date('01.04.2018', '02-APR')
        '2018-04-02T00:00:00'
    """
    # sanitize row date first
    row_date = row_date.replace('\'', '')

    if len(row_date.split('-')) == 2:
        day, month = row_date.split('-')

        if not month.isdigit():
            month = dt.datetime.strptime(month, '%b').month

        r_date, r_month, r_year = raw_reported_date.split('.')

        if (r_month == '1' or r_month == '01') and month == '12':
            row_date = dt.datetime(year=(int(r_year) - 1), month=int(month), day=int(day))
        elif '12' == r_month and (month == '1' or month == '01'):
            row_date = dt.datetime(year=(int(r_year) + 1), month=int(month), day=int(day))
        else:
            row_date = dt.datetime(year=(int(r_year)), month=int(month), day=int(day))

        return row_date.isoformat()

    else:
        raise ValueError(f"Portcall date is invalid {row_date}")


class ASLiquidExcelExtractor(GenericExcelExtractor):
    def parse_row(self, row):
        return row
