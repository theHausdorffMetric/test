from itertools import zip_longest
import logging
import re

from kp_scrapers.lib.date import get_date_range, is_isoformat, to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, is_number, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


BLACKLIST = [
    'various',
    'approx',
    'no working vessel',
    'revert',
    'vecant',
    'vacant',
    'passenger',
    'no calling',
    '-do-',
]
PRODUCT_REPLACEMENT = [
    ('hf-hsd', 'hsd'),
]
MISSING_ROWS = []
ZONE_MAPPING = {
    'budge budge': 'budge budge',
    'budge': 'budge budge',
    'haldia': 'haldia',
    'haldia - inside dock': 'haldia',
    'haldia-inside dock': 'haldia',
    'haldia - indise dock': 'haldia',
    'haldia-hoj 1': 'haldia',
    'paradeep': 'paradip',
    'vizag': 'visakhapatnam',
    'kakinada': 'kakinada',
    'krishnapatnam': 'krishnapatnam',
    'chennai': 'chennai',
    'ennore': 'ennore',
    'mangalore': 'mangalore',
    'mumbai': 'mumbai',
    'jnpt': 'jnpt',
    'mundra': 'mundra',
    'kandla': 'kandla',
    'sikka - rjmt': 'kandla',
    'sikka-gsfc': 'kandla',
    'dahej': 'dahej',
    'hazira': 'dahej',
}
MOVEMENT_MAPPING = {
    'l': 'load',
    '(l)': 'load',
    'exp': 'load',
    'imp': 'discharge',
    'd': 'discharge',
    '(d)': 'discharge',
    'to discharge': 'discharge',
    'to load': 'load',
    'disch': 'discharge',
    'disc': 'discharge',
    'dis': 'discharge',
}


@validate_item(CargoMovement, normalize=True, strict=True, log_level='error')
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]: normalized cargo movement item

    """
    item = map_keys(raw_item, field_mapping())
    print(item)

    # normalize berth strings
    if item.get('berth'):
        item['berth'] = item['berth'].replace('.', '')

    if item.get('vessel_name_length'):
        v_name, v_loa, v_beam = split_name_loa_beam(item.pop('vessel_name_length', None))
        item['vessel'] = {
            'name': v_name,
            'length': int(float(v_loa)) if is_number(v_loa) else None,
            'beam': int(float(v_beam)) if is_number(v_beam) else None,
        }
    else:
        _loa = item.pop('vessel_length', None)
        item['vessel'] = {
            'name': normalize_vessel(item.pop('vessel_name', None)),
            'length': int(float(_loa)) if is_number(_loa) else None,
        }

    # for kandla attachment, the remarks column does not reflect player information but
    # the berth information
    if 'kandla' in item['port_name']:
        item.pop('cargo_player', None)

    if not item['vessel']['name']:
        return

    # handle kandla attachment where berth dates potentially could be in 5 columns
    for berthed_col in ('berthed_1', 'berthed_2', 'berthed_3', 'berthed_4', 'berthed_5'):
        if item.get(berthed_col) and item[berthed_col]:
            item['berthed'] = item.get(berthed_col)
            item.pop(berthed_col, None)
        item.pop(berthed_col, None)

    for date_col in ('eta', 'arrival', 'berthed', 'departure', 'eta_holder'):
        if item.get(date_col):
            item[date_col] = normalize_dates(item[date_col], item['reported_date'])
            continue
        else:
            item[date_col] = None
            continue
    if item.get('eta_holder'):
        item['eta'] = item.get('eta_holder', None)

    if (
        not item.get('eta')
        and not item.get('berthed')
        and not item.get('arrival')
        and not item.get('departure')
    ):
        return

    cargo_products = None
    cargo_volume = None
    cargo_movement = None
    # handle normal cases where cargo and quantity are nicely seperated
    if item.get('cargo_product') and item.get('cargo_volume'):
        cargo_products = item.pop('cargo_product', None)
        cargo_volume = item.pop('cargo_volume', None)
        cargo_movement = item.pop('cargo_movement', None)

    # if cargo and volume are joint together
    if item.get('cargo_product_volume'):
        cargo_products, cargo_volume = split_product_vol(item.pop('cargo_product_volume', None))
        cargo_movement = item.pop('cargo_movement', None)

    if item.get('cargo_product') and not item.get('cargo_volume'):
        cargo_products, cargo_volume = split_product_vol(item.pop('cargo_product', None))
        cargo_movement = item.pop('cargo_movement', None)

    if not cargo_products:
        return

    zipped_cargo_list = normalize_prod_vol_move(cargo_products, cargo_volume, cargo_movement)
    seller = None
    buyer = None
    player = item.get('cargo_player') or item.get('potential_cargo_player') or None
    for col in ('cargo_player', 'potential_cargo_player', 'eta_holder'):
        item.pop(col, None)
    if zipped_cargo_list:
        for zipped_item in zipped_cargo_list:
            movement = MOVEMENT_MAPPING.get(zipped_item[2], None)
            if movement == 'load':
                seller = player
            elif movement == 'discharge':
                buyer = player
            else:
                item.pop('cargo_player', None)
            item['cargo'] = {
                'product': may_strip(zipped_item[0]),
                'movement': movement if movement else None,
                'volume': str(zipped_item[1]) if zipped_item[1] else None,
                'volume_unit': Unit.tons,
                'buyer': {'name': buyer} if buyer and buyer not in BLACKLIST else None,
                'seller': {'name': seller} if seller and seller not in BLACKLIST else None,
            }

            if item['cargo']['product'] in BLACKLIST:
                continue

            yield item

    else:
        MISSING_ROWS.append(str(raw_item))


def field_mapping():
    return {
        'arvd': ('arrival', None),
        'arrvng': ('arrival', None),
        'arvd/eta': ('arrival', None),
        'arrival': ('arrival', None),
        'arrived': ('arrival', None),
        'arvd or eta': ('arrival', None),
        'berth': ('berth', None),
        'berths': ('berth', None),
        'berth no': ('berth', None),
        'b pref': ('berth', None),
        'berth no.': ('berth', None),
        'suitable berth': ('berth', None),
        'berth or place': ('berth', None),
        'b.no': ('berth', None),
        'brthd / comm.': ('berthed', None),
        'berthng': ('berthed', None),
        'brthd': ('berthed', None),
        'berthed': ('berthed', None),
        'cargo': ('cargo_product', None),
        'cargo commodity': ('cargo_product', None),
        'cargo & qty': ('cargo_product_volume', None),
        'cgo/qty': ('cargo_product_volume', None),
        'cgo/qty in mts': ('cargo_product_volume', None),
        'eta/arvd': ('eta', None),
        'eta/anchored': ('eta', None),
        'eta_holder': ('eta_holder', None),
        'eta': ('eta', None),
        'etb': ('berthed', None),
        'etc': ('departure', None),
        'etd': ('departure', None),
        'etc/etd': ('departure', None),
        'etc / d': ('departure', None),
        'etc/d': ('departure', None),
        'jetty': ('berth', None),
        'j loa /draft': ignore_key('irrelavant'),
        'v loa /draft': (
            'vessel_length',
            lambda x: x.split('/')[0] if x and x not in BLACKLIST else None,
        ),
        'vessel': ('vessel_name', None),
        'vessel name': ('vessel_name', None),
        'name of the vessels': ('vessel_name', None),
        'vessel (loa)': ('vessel_name_length', None),
        'vessel (loa)': ('vessel_name_length', None),
        'vsl name': ('vessel_name', None),
        'vsls name': ('vessel_name', None),
        'imp/ exp': ('cargo_movement', None),
        'imp/exp': ('cargo_movement', None),
        'l / d': ('cargo_movement', None),
        'operation': ignore_key('irrelavant'),
        'd/l': ('cargo_movement', None),
        'l/d': ('cargo_movement', None),
        'ops (d/l)': ('cargo_movement', None),
        'quantity(mt)': ('cargo_volume', None),
        'qty (mt)': ('cargo_volume', None),
        'qnty(mt)': ('cargo_volume', lambda x: x.replace('--', '')),
        'cargo qty (in mts)': ('cargo_volume', None),
        'quantity (mt)': ('cargo_volume', None),
        'quantity': ('cargo_volume', None),
        'quantity-m/t': ('cargo_volume', None),
        'qty in mts': ('cargo_volume', None),
        'qty': ('cargo_volume', None),
        'rcvr/shpr': ('cargo_player', None),
        'shpr/rcvr': ('cargo_player', None),
        'shippers/receivers': ('cargo_player', None),
        'shprs / rcvrs': ('cargo_player', None),
        'shp/rcv': ('cargo_player', None),
        'remark': ignore_key('irrelavant'),
        'remarks': ('potential_cargo_player', None),
        'bal': ignore_key('irrelavant'),
        'l/d port': ignore_key('irrelavant'),
        '24 hrs l/d rate': ignore_key('irrelavant'),
        'name of vessel': ('vessel_name', None),
        'oj-1': ('berthed_1', None),
        'oj-2': ('berthed_2', None),
        'oj-3': ('berthed_3', None),
        'oj-4': ('berthed_4', None),
        'oj-5': ('berthed_5', None),
        'port_name': ('port_name', lambda x: ZONE_MAPPING.get(x, x)),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: extract_reported_date(x)),
    }


def split_name_loa_beam(raw_name_loa_beam):
    """split vessel name and loa if exists

    Args:
        vessel_name (str):

    Examples:
        >>> split_name_loa_beam('abc (160)  9.40 m ')
        ('abc', '160', '9.40')
        >>> split_name_loa_beam('abc (160)')
        ('abc', '160', '')
        >>> split_name_loa_beam('abc')
        ('abc', None, None)

    Returns:
        str:
    """
    list_vessel_prop = re.split(r'[\(\)]', raw_name_loa_beam)
    if len(list_vessel_prop) == 1:
        return normalize_vessel(list_vessel_prop[0]), None, None
    if len(list_vessel_prop) == 2:
        return (
            normalize_vessel(list_vessel_prop[0]),
            may_strip(re.sub('[m]', '', list_vessel_prop[1])),
            None,
        )
    if len(list_vessel_prop) == 3:
        return (
            normalize_vessel(list_vessel_prop[0]),
            may_strip(re.sub('[m]', '', list_vessel_prop[1])),
            may_strip(re.sub('[m]', '', list_vessel_prop[2])),
        )

    return None, None, None


def normalize_vessel(raw_vessel_name):
    """Normalize vessel.

    Args:
        vessel_name (str):

    Examples:
        >>> normalize_vessel('mv. abc')
        'abc'
        >>> normalize_vessel('mv abc')
        'abc'
        >>> normalize_vessel('mv. mbc')
        'mbc'
        >>> normalize_vessel('m.t.mbc')
        'mbc'
        >>> normalize_vessel('lpg/c mbc')
        'mbc'
        >>> normalize_vessel('m.t. mbc')
        'mbc'
        >>> normalize_vessel('mt "mbc"')
        'mbc'

    Returns:
        str:
    """
    if raw_vessel_name and raw_vessel_name not in BLACKLIST:
        _v_match = re.match(
            r'(?:mv\s|mv.\s|m.t.\s|mt.\s|mt\s|mt.|lpg/c.\s|lpg/c\s|m.t.\s?)?(.*)', raw_vessel_name
        )
        if _v_match:
            return may_strip(re.sub(r'[\"]', '', _v_match.group(1)))

        return raw_vessel_name
    return None


def normalize_dates(raw_date, rpt_date):
    """Normalize dates

    Args:
        vessel_name (str):

    Examples:
        >>> normalize_dates('1400 hrs 01.01.2020', '2020-01-01T00:00:00')
        '2020-01-01T14:00:00'
        >>> normalize_dates('2106 hrs/02.01.2020', '2020-01-01T00:00:00')
        '2020-01-02T21:06:00'
        >>> normalize_dates('2106 hrs /02.01.2020', '2020-01-01T00:00:00')
        '2020-01-02T21:06:00'
        >>> normalize_dates('02.01.2020(NOR)', '2020-01-01T00:00:00')
        '2020-01-02T00:00:00'
        >>> normalize_dates('02.01.2020', '2020-01-01T00:00:00')
        '2020-01-02T00:00:00'
        >>> normalize_dates('02.01.20', '2020-01-01T00:00:00')
        '2020-01-02T00:00:00'
        >>> normalize_dates('22.03.2020-1800', '2020-01-01T00:00:00')
        '2020-03-22T18:00:00'
        >>> normalize_dates('22.03.2020-AM', '2020-01-01T00:00:00')
        '2020-03-22T06:00:00'
        >>> normalize_dates(' am 02.01.2020', '2020-01-01T00:00:00')
        '2020-01-02T06:00:00'
        >>> normalize_dates('02.01.2020/0700 hrs', '2020-01-01T00:00:00')
        '2020-01-02T07:00:00'
        >>> normalize_dates('pm hrs 06.01.2020', '2020-01-01T00:00:00')
        '2020-01-06T15:00:00'
        >>> normalize_dates('26/01', '2020-01-01T00:00:00')
        '2020-01-26T00:00:00'
        >>> normalize_dates('26/01', '2019-12-30T00:00:00')
        '2020-01-26T00:00:00'

    Returns:
        str:

    """
    # normalize dates strings
    raw_date = raw_date.lower().replace('am', '0600/').replace('pm', '1500/')
    if is_isoformat(raw_date.upper()):
        return raw_date.upper()

    # if dd/mm data is provided, guess the year and return date
    if '/' in raw_date and len(raw_date.split('/')) == 2:
        if all(is_number(rd) for rd in raw_date.split('/')):
            potential_date, _ = get_date_range(raw_date, '/', '-', rpt_date)
            return potential_date

    # detect date and time fields
    date_hour = [
        may_strip(_d) for _d in re.split(r'(hrs /|hrs/|/ hrs|hrs|/|\()|\-', raw_date) if _d
    ]

    _date, _time = None, ''
    for dh in date_hour:
        if is_number(dh):
            _time = dh
            continue
        if len(dh.split('.')) == 3:
            _date = dh
            continue

    if not _date:
        return None

    try:
        return to_isoformat(may_strip(f'{_date} {_time}'), dayfirst=True)
    except Exception:
        return None


def split_product_vol(raw_prod_vol):
    """Split product and volumes if joined together to have a common
    seperator and clean noise

    Args:
        raw_prod_vol (str):

    Examples:
        >>> split_product_vol('edible oil/1000 mt')
        ('edible oil', '1000')
        >>> split_product_vol('diesel 11075/ ms 7294')
        ('diesel | ms', '11075|7294')
        >>> split_product_vol('edible oil/hsd/1000 mt')
        ('edible oil|hsd', '1000')
        >>> split_product_vol('edible oil/hsd/1000 mt/1000 mt')
        ('edible oil|hsd', '1000 |1000')
        >>> split_product_vol('21929 t butane/propane')
        ('butane/propane', '21929')
        >>> split_product_vol('21929 t butane')
        ('butane', '21929')
        >>> split_product_vol('lpg propane/butane (9500 mts)')
        ('lpg propane|butane', '9500')

    Returns:
        Tuple[str, str]:
    """
    for noisy_exp in BLACKLIST:
        raw_prod_vol = re.sub(noisy_exp, '', raw_prod_vol, flags=re.IGNORECASE)

    # occaional typo errors in report
    for incorrect_prod in PRODUCT_REPLACEMENT:
        raw_prod_vol = re.sub(
            incorrect_prod[0], incorrect_prod[1], raw_prod_vol, flags=re.IGNORECASE
        )

    # to handle example 2 above
    raw_prod_vol = re.sub(r'^(.*?)(\d+\/)([A-z0-9 ]+?)([0-9]+)$', r'\1/\3/\2\4', raw_prod_vol)

    if raw_prod_vol and ' t ' in raw_prod_vol:
        tokens = raw_prod_vol.split(' t ')
        return may_strip(tokens[1]), may_strip(tokens[0])

    if raw_prod_vol:
        tokens = re.split(r'[\/(]', raw_prod_vol)
        if len(tokens) % 2 == 0:
            products = '|'.join(tokens[: int(len(tokens) / 2)])
            volumes = '|'.join(tokens[int(len(tokens) / 2) :])

        if len(tokens) % 2 != 0:
            products = '|'.join(tokens[:-1])
            volumes = '|'.join(tokens[-1:])

        volumes = re.sub(r'(mts|mt|\(|\))', '', volumes)
        return may_strip(products), may_strip(volumes)

    return None, None


def normalize_prod_vol_move(raw_product, raw_volume_movement, raw_opt_movement):
    """split products using common seperator after being normalized
    in split_product_vol

    Args:
        raw_product (str):
        raw_volume_movement (str):

    Examples:
        >>> normalize_prod_vol_move('butane', '3000', 'd')
        [['butane', '3000', 'd']]
        >>> normalize_prod_vol_move('butane/propane', '3000', 'd')
        [['butane', 1500.0, 'd'], ['propane', 1500.0, 'd']]
        >>> normalize_prod_vol_move('butane/propane', '3000/3000', 'd')
        [['butane', '3000', 'd'], ['propane', '3000', 'd']]
        >>> normalize_prod_vol_move('propane', '6500 mt (d)', None)
        [['propane', '6500', 'd']]
        >>> normalize_prod_vol_move('toluene + mx', '3133+ 950 mt (d)', None)
        [['toluene ', '3133', ''], [' mx', '950', 'd']]
        >>> normalize_prod_vol_move('toluene + mx', '5694 mt (d) + 3640 mt (d)', None)
        [['toluene ', '5694', 'd'], [' mx', '3640', 'd']]

    Returns:
        Tuple[str, str]:
    """
    raw_volume_movement = raw_volume_movement.replace(',', '')
    product_list = re.split(r'[\\/,\&\+\|]', raw_product)
    volume_movement_list = re.split(r'[\\/,\&\+\|]', raw_volume_movement)
    volume_list, movement_list = extract_volume_movement(volume_movement_list)

    # overwrite movement list if the source has a movement col
    if raw_opt_movement:
        movement_list = [may_strip(raw_opt_movement) for mov_item in product_list]

    if len(product_list) == len(volume_list):
        zipped_list = [list(a) for a in list(zip_longest(product_list, volume_list, movement_list))]
        return zipped_list

    if len(product_list) != len(volume_movement_list) and len(volume_list) == 1:
        if is_number(volume_list[0]):
            volume = [float(volume_list[0]) / 2 for i in range(0, len(product_list))]
        else:
            volume = []
        zipped_list = [list(a) for a in list(zip_longest(product_list, volume, movement_list))]
        return zipped_list

    return None


def extract_volume_movement(vol_movement):
    """Normalize dates

    Args:
        raw_product (str):
        raw_volume_movement (str):

    Examples:
        >>> extract_volume_movement(['3000'])
        (['3000'], [''])
        >>> extract_volume_movement(['3640 mt (d)', '3640 mt (d)'])
        (['3640', '3640'], ['d', 'd'])
        >>> extract_volume_movement(['3640 mt', '3640 mt (d)'])
        (['3640', '3640'], ['', 'd'])
        >>> extract_volume_movement(['3640 mt'])
        (['3640'], [''])

    Returns:
        Tuple[str, str]:
    """
    movement_list = []
    vol_list = []
    for vm in vol_movement:
        if is_number(may_strip(vm)):
            vol_list.append(may_strip(vm))
            movement_list.append('')
        else:
            try:
                separated_vol_movement = vm.split('(')
                movement_list.append(re.sub(r'(\s|mt|\(|\))', '', separated_vol_movement[1]))
                vol_list.append(re.sub(r'(\s|mt|\(|\))', '', separated_vol_movement[0]))
            except Exception:
                vol_list.append(re.sub(r'(\s|mt|\(|\))', '', vm))
                movement_list.append('')

    return vol_list, movement_list


def extract_reported_date(raw_reported_date):
    """raw_reported_date

    Args:
        raw_reported_date (str):

    Returns:
        str:

    """
    _match = re.match(r'.*\s(\d{1,2}\.\d{1,2}\.\d{2,4})', raw_reported_date)
    if _match:
        return to_isoformat(_match.group(1), dayfirst=True)
