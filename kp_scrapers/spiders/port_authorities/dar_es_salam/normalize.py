import datetime as dt
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import get_first_day_of_next_month, to_isoformat
from kp_scrapers.lib.parser import may_strip, str_to_float
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

CARGO_BLACKLIST = [
    'EX ',
    'TICTS',
    'TPA',
    'BALLAST',
    'CONTAINERS',
    'GENERAL CARGO',
    r'FOR\s*TPA\s*PROJECT',
    r'WAITING\s*TO\s*SAIL',
    r'NAVY\s*SHIP',
    r'FISHING\s*VESSEL',
    r'MOTOR\s*VEHICLES',
]

RELEVANT_VESSEL_TYPES = ['GC', 'T']

NO_VALUE_SIGN = ['', '-', 'TBA']


@validate_item(PortCall, normalize=True, strict=False)
def process_eta_item(raw_item):
    """Transform raw item into an event.

    This is for EXPECTED ARRIVALS table.

    Args:
        raw_item (Dict[str]):

    Returns:
        Dict[str, str | Dict[str, str]]:

    """
    item = map_keys(
        raw_item, eta_field_mapping(reported_date=raw_item['reported_date']), skip_missing=True
    )

    # ignore irrelevant vessel type (agent contains vessel type info)
    if item['agent_cargo_receiver'][1] not in RELEVANT_VESSEL_TYPES:
        logger.info(
            f'Vessel is of an irrelevant type: '
            f'{item["vessel_name"]} ({item["agent_cargo_receiver"][1]})'
        )
        return

    # ignore vessel with irrelevant cargo and empty cargo
    cargoes = list(normalize_cargoes_in_expected_arrival(item['agent_cargo_receiver'][2]))
    if not cargoes:
        logger.info(f'Vessel is carrying irrelevant cargo: {item["vessel_name"]}')
        return

    return {
        'port_name': item['port_name'],
        'provider_name': item['provider_name'],
        'reported_date': item['reported_date'],
        'eta': item['eta'],
        'vessel': {
            'name': item['vessel_name'],
            'gross_tonnage': item['gross_tonnage'],
            'length': item['vessel_length'],
        },
        'cargoes': cargoes,
    }


@validate_item(PortCall, normalize=True, strict=False)
def process_at_berth_item(raw_item):
    """Transform item into a port call event (arrival).

    Args:
        raw_item (Dict[str]):

    Returns:
        Dict[str, str | Dict[str, str]]:

    """
    cargoes = list(normalize_cargoes_in_berth_plan(raw_item, ['cargo', 'import', 'export']))
    if not cargoes:
        return

    item = map_keys(raw_item, at_berth_field_mapping(), skip_missing=True)
    return {
        'port_name': item['port_name'],
        'provider_name': item['provider_name'],
        'reported_date': item['reported_date'],
        'berthed': guess_at_berth_berthed_date(item['reported_date']),
        'vessel': {'name': item['vessel_name'], 'length': item['vessel_length']},
        'cargoes': cargoes,
    }


@validate_item(PortCall, normalize=True, strict=False)
def process_anchorage_item(raw_item):
    """Transform item into a port call event (berthed).

    Args:
        raw_item (Dict[str]):

    Returns:
        Dict[str, str | Dict[str, str]]:

    """
    cargoes = list(normalize_cargoes_in_berth_plan(raw_item, ['TYPE OF CARGO', 'IMPORT', 'EXPORT']))
    if not cargoes:
        return

    item = map_keys(raw_item, anchorage_field_mapping(), skip_missing=True)

    item['arrival'] = normalize_arrival_date(item.pop('arrival_date'), item.pop('arrival_time'))
    if not item['arrival']:
        return

    item['vessel'] = {'name': item.pop('vessel_name'), 'length': item.pop('vessel_length')}

    return item


def eta_field_mapping(**kwargs):
    return {
        '0': ('eta', lambda x: normalize_date(x, **kwargs)),
        '1': ignore_key('draught'),
        '2': ('vessel_length', lambda x: x if x else None),
        '3': ('gross_tonnage', None),
        '4': ('vessel_name', normalize_vessel_name),
        '5': ('agent_cargo_receiver', split_agent_cargo_receiver),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
    }


def at_berth_field_mapping():
    return {
        # vessel
        'vessel_name': ('vessel_name', normalize_vessel_name),
        'ship_draught': ignore_key('draught'),
        'ship_length': ('vessel_length', lambda x: x if x else None),
        # port
        'berth': ('berth', None),
        # meta field
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
    }


def anchorage_field_mapping():
    return {
        # vessel
        'SHIP  NAME': ('vessel_name', normalize_vessel_name),
        'DRAFT/LOA': ('vessel_length', normalize_vessel_length),
        # arrival date
        'SIT.DATE': ('arrival_date', None),
        'TIME0': ('arrival_time', None),
        # meta field
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
    }


def normalize_arrival_date(date, time):
    """Combine date and time, convert them to ISO 8601 format.

    Examples:
        >>> normalize_arrival_date('09.09.18', '0500')
        '2018-09-09T05:00:00'
        >>> normalize_arrival_date('09.09.18', '-')
        '2018-09-09T00:00:00'
        >>> normalize_arrival_date('-', '-')

    Args:
        date (str):
        time (str):

    Returns:
        str:

    """
    if date in NO_VALUE_SIGN:
        return None

    if time in NO_VALUE_SIGN:
        return to_isoformat(date, dayfirst=True)

    return to_isoformat(' '.join([date, time]), dayfirst=True)


def normalize_date(date_str, reported_date):
    """Normalize date to ISO 8601 format.

    The date_str doesn't contain month and year info, so we need reported date as reference. If the
    day is no bigger than reported date, then it's the same month and year as reported date. If the
    day is smaller than reported date, then it's next month.

    Examples:
        >>> normalize_date('30TH MON', '23RD JULY 2018')
        '2018-07-30T00:00:00'
        >>> normalize_date('2ND THU', '23RD JULY 2018')
        '2018-08-02T00:00:00'

    Args:
        date_str (str): format as 24TH TUE
        reported_date (str): format as 23RD JULY 2018

    Returns:
        str:

    """
    reported_date = parse_date(reported_date)
    day, month, year = reported_date.day, reported_date.month, reported_date.year
    eta_day = int(re.search(r'\d*', date_str).group())

    if eta_day >= day:
        return dt.datetime(year, month, eta_day).isoformat()
    else:
        next_date = get_first_day_of_next_month(reported_date)
        return dt.datetime(next_date.year, next_date.month, eta_day).isoformat()


def normalize_cargoes_in_expected_arrival(raw_cargo):
    """Extract cargoes information from raw item (from Expected Arrival table).

    Args:
        raw_cargo (Dict[str, str]):

    Returns:
        List[Dict(str, str)]:

    """
    cargo_info = _handel_specific_cargo(raw_cargo)
    for alias in CARGO_BLACKLIST:
        if any(alias in product for product in cargo_info):
            logger.info(f'Cargo is blacklisted, will not yield: {cargo_info}')
            return None

    for cargo in cargo_info:
        yield _assemble_cargo(cargo, None, None)


def _assemble_cargo(product, movement, volume):
    return {
        'product': may_strip(product),
        'movement': movement,
        'volume': volume,
        'volume_unit': 'tons',
    }


def normalize_cargoes_in_berth_plan(raw_item, key_list):
    """Extract cargoes information from raw item (from Berth Plan table).

    Args:
        raw_item (Dict[str, str):
        key_list (List[str]):

    Returns:
        List[Dict[str, str]] | None:

    """

    cargo_info = raw_item[key_list[0]].strip()
    discharge = raw_item[key_list[1]]
    load = raw_item[key_list[2]]

    for block in CARGO_BLACKLIST:
        if re.search(block, cargo_info):
            return

    # handle cargo info like 'JET A1 / 1K'
    cargo_info = _handel_specific_cargo(cargo_info)

    if cargo_info in NO_VALUE_SIGN:
        return

    if str_to_float(discharge):
        disch = str_to_float(discharge) / len(cargo_info)
        for cargo in cargo_info:
            yield _assemble_cargo(cargo, 'discharge', str(disch))

    if str_to_float(load):
        load = str_to_float(load) / len(cargo_info)
        for cargo in cargo_info:
            yield _assemble_cargo(cargo, 'load', str(load))


def _handel_specific_cargo(info):
    """Handle cargo information.

     1. cargo/receiver: JET A1 / 1K

    Args:
        info (str):

    Returns:
        List[str]

    """
    if '/' in info:
        return info.split('/')

    return [info]


def normalize_vessel_name(vessel_name):
    """Omit 'MT' at the beginning and remove double-spaces.

    Examples:
        >>> normalize_vessel_name('MT GRAND ACE 11')
        'GRAND ACE 11'
        >>> normalize_vessel_name('MTAMT ')
        'MTAMT'

    Args:
        vessel_name (str):

    Returns:
        str:

    """
    for prefix in ('MT ', 'MSC '):
        if vessel_name.startswith(prefix):
            vessel_name = vessel_name.replace(prefix, '')
    return may_strip(vessel_name)


def split_agent_cargo_receiver(strs):
    """Split AGENT  /  CARGO  /  RECEIVER field.

    Usually, the format is:
    Agent - Cargo
    Agent - Receiver

    So, we can split the string by '  - ' and retrieve the two parts. Agent may contains -, but the
    number of blanks near the dash is 1, unlike the separator: 2. Thus we can make sure it's
    divided correctly.

    Examples:
        >>> split_agent_cargo_receiver('SEAFORTH  ( GC )   -  WHEAT  IN  BULK   ( S.S.B )')
        ('SEAFORTH', 'GC', 'WHEAT IN BULK')
        >>> split_agent_cargo_receiver('MESSINA  ( RO-RO )  -  TPA  ( TO DISCH. 91 UNITS )')
        ('MESSINA', 'RO-RO', 'TPA')
        >>> split_agent_cargo_receiver('SEAFORTH  ( GC )   - JET A1 / IK')
        ('SEAFORTH', 'GC', 'JET A1 / IK')

    Args:
        strs (str):

    Returns:
        Tuple[str, str, str]: a tuple of (agent, vessel_type, cargo/receiver)

    """
    str_match = re.match(r'^([\w\s]+)\(\s*(\S+)\s*\)\s*\-\s*([\w\s\/\-]+)', strs)
    return tuple(map(may_strip, list(str_match.groups()))) if str_match else (None, None, None)


def guess_at_berth_berthed_date(reported_date):
    """Arrival date would be a previous day of reported date.

    Args:
        reported_date (str):

    Returns:
        str: date in format of ISO 8601

    """
    return (parse_date(reported_date, dayfirst=False) - dt.timedelta(days=1)).isoformat()


def normalize_vessel_length(raw_length):
    """Extract vessel length from DRAFT/LOA field.

    Examples:
        >>> normalize_vessel_length('9.8 (183.06)')
        '183.06'
        >>> normalize_vessel_length('8.2 (240)')
        '240'

    Args:
        raw_length (str):

    Returns:
        str | None:

    """
    length_match = re.search(r'\((.*)\)', raw_length)
    return length_match.group(1).strip() if length_match else None
