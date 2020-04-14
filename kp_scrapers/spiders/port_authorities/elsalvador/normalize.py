import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

IRRELEVANT_CARGOES = ['CONT.', 'CONTENEDORES', 'VEHICULOS']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    item = map_keys(raw_item, field_mapping())

    cargoes = list(build_cargo(item))
    if not cargoes:
        logger.info(f'discarding vessels without cargoes: {item["vessel_name"]}')
        return

    return {
        'cargoes': cargoes,
        'eta': to_isoformat(' '.join([item['eta_day'], item['eta_hours']])),
        'port_name': item['port_name'],
        'provider_name': item['provider_name'],
        'reported_date': item['reported_date'],
        'vessel': {'name': item['vessel_name'], 'gross_tonnage': item.get('vessel_gt')},
    }


def field_mapping():
    return {
        'vessel_name': ('vessel_name', normalize_vessel_name),
        'vessel_draft': ignore_key('not used in model'),
        'vessel_length': ignore_key('not used in model'),
        'vessel_gt': ('vessel_gt', lambda x: may_strip(x[0]).replace(',', '')),
        'shipping_agent': ignore_key('not used in model'),
        'eta_day': ('eta_day', lambda x: may_strip(x[0])),
        'eta_hours': ('eta_hours', validate_hour),
        'is_discharge': ('is_discharge', lambda x: may_strip(x[0])),
        'is_load': ('is_load', lambda x: may_strip(x[0])),
        'tons_moved': ('tons_moved', lambda x: [may_strip(i).replace(',', '') for i in x]),
        'units_moved': ('units_moved', lambda x: [may_strip(i).replace(',', '') for i in x]),
        'operation_time': ignore_key('not used in model'),
        'expected': ignore_key('not used in model'),
        'client': ignore_key('not used in model'),
        'cargo': ('cargo', None),
        # transfer static data
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_vessel_name(vessel_name):
    """Normalize and cleanup vessel name.

    Args:
        vessel_name (List[str]): list of possible vessels

    Returns:
        str:

    Examples:
        >>> normalize_vessel_name(['4.12173 - NORDIC MARITA'])
        'NORDIC MARITA'
    """
    return may_strip(vessel_name[0].split('-', 1)[1])


def validate_hour(hour):
    """Return hour if is valid hour for converting to isoformat, else empty string.

    Adds a space in front, for `to_isoformat` to parse.
    Returns empty string otherwise will fail downstream

    Args:
        hour (List[str]):

    Returns:
        str:

    Examples:
        >>> validate_hour(['12:00:00'])
        '12:00:00'
        >>> validate_hour(['PM.'])
        ''
        >>> validate_hour(['\xa0'])
        ''
        >>> validate_hour(['18:00'])
        '18:00'
    """
    hour = may_strip(hour[0])
    return hour if re.match(r'(\d{2}[:\d{2}]+)', hour) else ''


def build_cargo(item):
    """Build cargo as specified by `models`

    Cells are extracted upstream as lists, as there can be more than one type of cargo.
    However, the table can repeat the cargo more than once as they could be from different clients.
    We still yield them as separate cargoes separately.

    Args:
        item (Dict[str, str]):

    Yields:
        Dict[str, str]:
    """
    for idx, product in enumerate(item['cargo']):
        # filter out containers
        if any(alias in product for alias in IRRELEVANT_CARGOES):
            logger.info(f'discarding irrelavent cargo: {product}')
            continue

        cargo = {'product': product, 'movement': 'discharge' if item['is_discharge'] else 'load'}

        # append volume info if present, sometimes it wont be
        if idx < len(item['tons_moved']) and item['tons_moved'][idx]:
            cargo.update(volume=item['tons_moved'][idx], volume_unit='tons')

        yield cargo
