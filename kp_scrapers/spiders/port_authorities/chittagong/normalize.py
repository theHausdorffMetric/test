import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_remove_substring, may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item, event_type):
    """Transform raw item into a usable event.

    Args:
        raw_item (dict[str, str]):
        event_type (str):

    Yields:
        PortCall validated model (dict):

    """
    portcall = {}
    if event_type == 'eta':
        item = map_keys(raw_item, eta_field_mapping())
        portcall.update({'eta': item['eta']})

    elif event_type == 'berthed':
        item = map_keys(raw_item, berthed_field_mapping())
        # berthed date pattern is dd/mm, such as 08/10
        berth = to_isoformat(item['berthed_date'] + '/' + item['reported_date'][:4])
        portcall.update({'berthed': berth})

    elif event_type == 'arrived':
        item = map_keys(raw_item, arrived_field_mapping())
        arrival = normalize_arrival_date(item['arrival'], item['reported_date'][:4])
        portcall.update({'arrival': arrival})

    vessel = init_vessel(item['vessel'])
    if item['vessel_length'].isdigit():
        vessel['length'] = item['vessel_length']
    if not vessel:
        logger.info(f"Vessel {item['vessel']} did not provide IMO number, discarding")
        return
    portcall.update(
        vessel=vessel,
        cargoes=item['cargo'],
        reported_date=item['reported_date'],
        port_name=item['port_name'],
        provider_name=item['provider_name'],
    )

    return portcall


def berthed_field_mapping():
    return {
        'NAMEOFVESSEL': ('vessel', lambda x: may_remove_substring(x, ['*'])),
        'LENGTH': ('vessel_length', lambda x: x.split('.')[0]),
        'CARGO': ('cargo', lambda x: [{'product': x}]),
        'LASTPORT': ('last_port', None),
        'FLAG': ('flag', None),
        'LOCALAGENT': ('shipping_agent', None),
        'DTOFARRIVAL': ignore_key('arrival date, not in use'),
        'BERTHING': ('berthed_date', None),
        'LEAVING': ignore_key('leaving_date, not in use'),
        'IMPORTDISCH': ignore_key('draught, not in use'),
        # static information
        'port_name': ('port_name', None),
        'reported_date': ('reported_date', None),
        'provider_name': ('provider_name', None),
    }


def eta_field_mapping():
    return {
        'NAMEOFVESSELS': ('vessel', None),
        'LENGTH': ('vessel_length', lambda x: x.split('.')[0]),
        'DRAFT': ignore_key('draft'),
        'DATEOFARRIVAL': ('eta', to_isoformat),
        'FLAG': ('flag', None),
        'LOCALAGENT': ('shipping_agent', None),  # not sure if local = shipping
        'LINEBELONGTO': ignore_key('belong_to, not in use'),
        'CARGOCARRIED': ('volume', None),  # cargo volume is useless w/o movement, not used.
        'TYPEOFCARGO': ('cargo', lambda x: [{'product': x}]),
        # static information
        'port_name': ('port_name', None),
        'reported_date': ('reported_date', None),
        'provider_name': ('provider_name', None),
    }


def arrived_field_mapping():
    return {
        'NAMEOFVESSEL': ('vessel', None),
        'R/ON': ignore_key('r/on, not in use'),
        'LENGTH': ('vessel_length', lambda x: x.split('.')[0]),
        'CARGO': ('cargo', lambda x: [{'product': x}]),
        'LPORT': ('last_port', None),
        'FLAG': ('flag', None),
        'ARRIVALDATETIME': ('arrival', lambda x: may_remove_substring(x, ['R/A(', ')'])),
        # static information
        'port_name': ('port_name', None),
        'reported_date': ('reported_date', None),
        'provider_name': ('provider_name', None),
    }


def init_vessel(vessel_name):
    """Build vessel information from raw vessel name.

    Args:
        vessel_name (str):

    Returns:
        Dict[str, str] | None: vessel dict if IMO present, else None

    Examples:
        >>> init_vessel('POLA ANISIA (IMO:9303869)')
        {'name': 'POLA ANISIA', 'imo': '9303869'}
        >>> init_vessel('VANTAGE REEF')
        {'name': 'VANTAGE REEF'}
    """
    if 'IMO:' in vessel_name:
        imo = may_strip(vessel_name.split('IMO:')[1].split(')')[0])
        if imo.isdigit():
            return {'name': may_strip(vessel_name.split('(')[0]), 'imo': imo}
        else:
            return {'name': may_strip(vessel_name.split('(')[0])}
    else:
        vessel_name = may_strip(vessel_name.split('(')[0])
        if vessel_name != '':
            return {'name': vessel_name}
        else:
            return None


def normalize_arrival_date(date_str, year):
    """Normalize arrival date, handle invalid date, with reported year as reference.

    Arrival date pattern is:
        - 11/10 AT 2200

    However, as we spotted several date as 08/10 AT 1278, so we need to check if the time is valid.

    Examples:
        >>> normalize_arrival_date('11/10 AT 2200B', '2018')
        '2018-10-11T22:00:00'
        >>> normalize_arrival_date('08/10 AT 1278', '2018')
        '2018-10-08T00:00:00'
        >>> normalize_arrival_date('24/10 AT 2400', '2018')
        '2018-10-24T00:00:00'

    Args:
        date_str (str):
        year (str):

    Returns:
        str: ISO 8601 format arrival date

    """
    date_str = date_str.replace('AT ', '')
    _match = re.match(r'(\d{1,2}\/\d{1,2}) (\d{2})(\d{2})', date_str)
    if not _match:
        logger.exception(f'Spotted new arrival date pattern: {date_str}')
        return

    date, hour, minute = _match.groups()
    # check if time is validate

    if int(hour) >= 24 or int(minute) >= 60:
        logger.info(f'Time is invalid, discard it: {date_str}')
        hour, minute = '', ''

    return to_isoformat(f'{date} {year} {hour}{minute}', dayfirst=True)
