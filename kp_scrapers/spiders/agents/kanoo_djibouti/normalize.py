import datetime as dt
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


BLACKLIST_CARGO = [
    'CONFIRM',
    'MVTS',
    'SUPPLY',
    'INSPECTION',
    'CAMEL',
    'BUNKERING',
    'ORDERS',
    'MAINTENANCE',
    'CHANGE',
    'PROBLEM',
    'ACCESSOIRES',
]


TOKENS_IGNORE = ['MT', 'OF', 'BULK', 'IN']


MOVEMENT_MAPPING = {'D': 'discharge', 'L': 'load'}


UNIT_MAPPING = {'MT': Unit.tons}


@validate_item(CargoMovement, normalize=True, strict=False, log_level='error')
def process_item(raw_item):
    """Transform raw item to Portcall model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, field_mapping())

    # discard portcall if no relevant vessel found
    if not item.get('vessel_name') or any(
        sub in item['vessel_name'] for sub in ('NIL', 'NAVY VESSEL')
    ):
        return

    item['vessel'] = {'name': item.pop('vessel_name', None), 'length': item.pop('vessel_loa', None)}

    if item.get('arrival_date'):
        item['arrival'] = normalize_date_time(
            item.pop('arrival_date', None), item.pop('month_row', None), item['reported_date']
        )

    if item.get('departure_date'):
        item['departure'] = normalize_date_time(
            item.pop('departure_date', None), item.pop('month_row', None), item['reported_date']
        )

    # discard portcall if no relevant portcall date found
    if not (item.get('eta') or item.get('arrival') or item.get('berthed') or item.get('departure')):
        return

    if item.get('cargo_information'):
        for f_cargo in split_cargo_volume(item.pop('cargo_information')):
            f_cargo_volume = f_cargo[1].replace(' ', '')
            item['cargo'] = {
                'product': clean_product(f_cargo[3]),
                'movement': MOVEMENT_MAPPING.get(may_strip(f_cargo[0]), None),
                'volume': may_strip(f_cargo_volume) if f_cargo_volume else None,
                'volume_unit': UNIT_MAPPING.get(may_strip(f_cargo[2]), None),
            }

            # discard null products
            if not item['cargo']['product']:
                continue
            yield item


def field_mapping():
    return {
        'ARRIVAL DATE': ('arrival_date', may_strip),
        'ARRIVAL': ('arrival_date', may_strip),
        'DEPART': ('departure_date', may_strip),
        'DEPARTURE': ('departure_date', may_strip),
        'AD': ignore_key('after draught'),
        'STEVEDORES': ignore_key('stevedores'),
        'PDSA': ignore_key('pds'),
        'DAILY': ignore_key('daily'),
        'ROB': ignore_key('rob'),
        'SHIP NAME': ('vessel_name', None),
        'VESSELS': ('vessel_name', None),
        'VESSEL': ('vessel_name', None),
        'VOY': ignore_key('voyage number'),
        'LOA': (
            'vessel_loa',
            lambda x: round(try_apply(x, float, int)) if try_apply(x, float, int) else None,
        ),
        'DRAFT': ignore_key('draft'),
        'FLAG': ignore_key('flag'),
        'AGENT': ('shipping_agent', lambda x: None if 'TBC' in x else x),
        'AGENTS': ('shipping_agent', lambda x: None if 'TBC' in x else x),
        'LINE': ignore_key('line'),
        'BERTH': ('berth', None),
        'OPERATIONS': ('cargo_information', lambda x: normalize_cargos(x)),
        'CALL': ignore_key('call'),
        'reported_date': ('reported_date', None),
        'provider_name': ('provider_name', None),
        'port_name': ('port_name', None),
        'month_row': ('month_row', None),
    }


def normalize_date_time(raw_date, raw_infer_month, raw_reported_date):
    """dates do not contain the year

    Format:
    1. dd/hhmm
    2. dd/bb AT hhmm
    3. dd/bb/yy AT hhmm

    Args:
        raw_date (str):
        raw_reported_date (str):

    Examples:
        >>> normalize_date_time('17/0600', 'SGTD AUGUST','2019-07-03T10:01:00')
        '2019-08-17T06:00:00'
        >>> normalize_date_time('17/0600', 'SGTD JULY','2019-07-03T10:01:00')
        '2019-07-17T06:00:00'
        >>> normalize_date_time('17/0600', 'SGTD DECEMBER','2019-12-03T10:01:00')
        '2019-12-17T06:00:00'
        >>> normalize_date_time('17/0600', 'SGTD JANUARY','2019-12-30T10:01:00')
        '2020-01-17T06:00:00'
        >>> normalize_date_time('17/0600', 'S bc','2019-12-03T10:01:00')
        '2019-12-17T06:00:00'
        >>> normalize_date_time('17/0600', 'S bc','2019-12-03T10:01:00')
        '2019-12-17T06:00:00'
        >>> normalize_date_time('29/05 AT 1300', 'S bc','2019-05-17T10:01:00')
        '2019-05-29T13:00:00'
        >>> normalize_date_time('28/03/18 AT 1600', 'S bc','2019-05-17T10:01:00')
        >>> normalize_date_time('23/11 /15 AT 1300', 'S bc','2019-05-17T10:01:00')

    Returns:
        str:
    """
    # get year and month from reported date
    rpt_year = parse_date(raw_reported_date).year
    rpt_month = parse_date(raw_reported_date).month

    # check if inferred month is present
    inferred_month = None
    try:
        if len(raw_infer_month.split(' ')) == 2:
            _, inferred_month = raw_infer_month.split(' ')
            inferred_month = parse_date(inferred_month).month
    except Exception:
        inferred_month = None
        pass

    _match = re.findall(
        r'^(?P<day>\d{1,2})\/(?P<month>\d{2}|AM|PM)?(?:[A-z\s]+)?(?P<time>\d{4}|AM|PM)$', raw_date
    )

    if len(_match) > 0:
        _day, e_month, _time = _match[0]

        # determine month to be used
        if inferred_month:
            _month = inferred_month
        elif e_month:
            _month = e_month
        else:
            _month = rpt_month

        f_time = normalize_time(_time)

        # handle roll over dates
        try:
            f_date = dt.datetime(year=int(rpt_year), month=int(_month), day=int(_day))
        except Exception:
            f_date = dt.datetime(year=int(rpt_year), month=int(_month) - 1, day=int(_day))

        # to accomodate end of year parsing, prevent dates too old or far into
        # the future. 100 days was chosen as a gauge
        if (f_date - parse_date(raw_reported_date)).days < -100:
            f_date = dt.datetime(year=int(rpt_year) + 1, month=int(_month), day=int(_day))

        if (f_date - parse_date(raw_reported_date)).days > 100:
            f_date = dt.datetime(year=int(rpt_year) - 1, month=int(_month), day=int(_day))

        return dt.datetime.combine(f_date, f_time).isoformat()

    logger.warning('unable to parse %s', raw_date)
    return None


def normalize_time(raw_time_string):
    """times are in a seperate column

    Format:
    1. hhss

    Args:
        raw_time_string (str):

    Examples:
        >>> normalize_time('0407')
        datetime.time(4, 7)

    Returns:
        Tuple[str, str]:
    """
    if raw_time_string:
        if raw_time_string.isdigit():
            return dt.time(int(raw_time_string[:2]), int(raw_time_string[2:]))
        if raw_time_string == 'AM':
            return dt.time(hour=6)
        if raw_time_string == 'PM':
            return dt.time(hour=18)

    return dt.time(hour=0)


def split_cargo_volume(raw_cargo_information):
    """split cargo, movement and volume

    Args:
        raw_cargo (str):
        raw_vol (str):

    Examples:
        >>> split_cargo_volume('D 28 278 MT STEAM COAL IN BULK')
        [('D', ' 28 278 ', 'MT', ' STEAM COAL IN BULK')]
        >>> split_cargo_volume('D 42 692 MT GASOIL')
        [('D', ' 42 692 ', 'MT', ' GASOIL')]
        >>> split_cargo_volume('D 28 047 GASOIL+7 984 JET A1')
        [('D', ' 28 047 ', '', 'GASOIL'), ('', '7 984 ', '', 'JET A1')]
        >>> split_cargo_volume('D 9387 GASOLINE + 984 MT GASOIL')
        [('D', ' 9387 ', '', 'GASOLINE'), ('', '984 ', 'MT', ' GASOIL')]
        >>> split_cargo_volume('CONTAINERS')
        []
        >>> split_cargo_volume('')

    Returns:
        List[Tuple[str]]:
    """
    if raw_cargo_information:
        information_list = [may_strip(info) for info in re.split('[\&\+]', raw_cargo_information)]
        final_list = []

        for val in information_list:
            _match = re.findall(
                '(?P<movement>L|D)?(?P<volume>[0-9.\s]+)(?P<units>MT|CTRS|UNITS|UNIT)?(?P<product>.*)',  # noqa
                val,
            )

            if len(_match) > 0:
                final_list.append(_match[0])
        return final_list
    return None


def normalize_cargos(raw_cargo_string):
    """remove unwanted cargos

    Args:
        raw_cargo_string (str):

    Examples:
        >>> normalize_cargos('1112 MVTS ')
        >>> normalize_cargos('D 9387 GASOLINE + 984 MT GASOIL')
        'D 9387 GASOLINE + 984 MT GASOIL'

    Returns:
        str:
    """
    if raw_cargo_string:
        if any(sub in may_strip(raw_cargo_string) for sub in BLACKLIST_CARGO):
            return None
        return raw_cargo_string
    return None


def clean_product(c_prod):
    """further sanitize the product after spliting

    Args:
        raw_cargo_string (str):

    Examples:
        >>> clean_product('OF LPG GAS')
        'LPG GAS'
        >>> clean_product(' MT GASOLINE')
        'GASOLINE'

    Returns:
        str:
    """
    if c_prod:
        c_prod = [token for token in c_prod.split(' ') if token.upper() not in TOKENS_IGNORE]
        return may_strip(' '.join(c_prod))
    return c_prod
