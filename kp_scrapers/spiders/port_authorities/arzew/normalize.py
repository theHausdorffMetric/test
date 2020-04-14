import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, split_by_delimiters, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


DATE_PARSING_PATTERN = r'(\d{4}-\d{2}-\d{2})\s*-?(\d{2}|[a-zA-Z]+)?\s*:?\s*-?(\d{2})?'

IRRELEVANT_CARGO = ['-', 'autres', 'conteneur', 'divers', 'marchandise']

MOVEMENT_MAPPING = {'Export': 'load', 'Import': 'discharge', '-': None}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    item = map_keys(raw_item, portcall_mapping())

    # discard portcall if no dates provided
    if not item.get('berthed') and not item.get('eta'):
        return

    # assign proper portcall event date, based on source page
    item['arrival'] = item.pop('eta') if 'rade' in item.pop('url') else None

    # discard irrelevant cargoes/vessels
    if not item['cargo_product']:
        logger.info(f'Vessel {raw_item["Navire"]} has irrelevant cargo: {raw_item["Marchandise"]}')
        return

    # build cargo sub-model
    product = item.pop('cargo_product')
    movement = item.pop('cargo_movement', None)
    volume = item.pop('cargo_volume', None)
    _safe_volume = safe_infer_volume(volume, movement)
    item['cargoes'] = [
        {
            'product': prod,
            'movement': movement,
            'volume': _safe_volume if _safe_volume else None,
            'volume_unit': Unit.tons if _safe_volume else None,
        }
        for prod in split_by_delimiters(product, '+')
    ]

    return item


def portcall_mapping():
    return {
        'Consignataire': ignore_key('shipping agent'),
        'Date et Heure d\'entrée': ('berthed', sanitize_date),
        'Date et Heure d\'arrivée': ('eta', sanitize_date),
        'Date et Heure de départ': ignore_key('estimated date of departure'),
        'Déstination': ignore_key('next port of call'),
        'Marchandise': (
            'cargo_product',
            lambda x: None if any(alias in x.lower() for alias in IRRELEVANT_CARGO) else x,
        ),
        'Motif': ignore_key('current vessel operation'),
        'Nature': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x)),
        'Navire': ('vessel', lambda x: {'name': x}),
        'Opération': ignore_key('duplicate of "Nature"'),
        'port_name': ('port_name', None),
        'Poste': ignore_key('berth'),
        'Provenance': ignore_key('preceding port of call'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'Tonnage': ('cargo_volume', None),
        'url': ('url', None),
    }


def sanitize_date(raw_date):
    """Sanitise raw date as ISO8601 timestamp.

    Raw date is usually on 2 lines.
    The first one being a nicely formatted %y-%m-%d.
    The second one being a messy combination of hours, AM, PM, ...

    The date could be invalid, in this case, we want to keep calm and keep processing.

    Examples:
        >>> sanitize_date('2019-05-11')
        '2019-05-11T00:00:00'
        >>> sanitize_date('2019-05-0907:20')
        '2019-05-09T07:20:00'
        >>> sanitize_date('2019-05-19      19:00')
        '2019-05-19T19:00:00'
        >>> sanitize_date('2019-05-11 AM:-')
        '2019-05-11T00:00:00'
        >>> sanitize_date('2019-05-05 09  :00')
        '2019-05-05T09:00:00'
        >>> sanitize_date('2019-05-10 :')
        '2019-05-10T00:00:00'
        >>> sanitize_date('2019-06-01 sh:')
        '2019-06-01T00:00:00'
        >>> sanitize_date('2019-06-01 -06:-00')
        '2019-06-01T06:00:00'
        >>> sanitize_date('')

    """
    # sanity check
    if not raw_date:
        return None

    # remove excessive whitespace first
    raw_date = may_strip(raw_date)
    try:
        date, hour, minute = re.match(DATE_PARSING_PATTERN, raw_date).groups()
        hour = try_apply(hour, int) if try_apply(hour, int) else '00'
        minute = try_apply(minute, int) if try_apply(minute, int) else '00'
        return to_isoformat(f'{date} {hour}:{minute}', dayfirst=False)

    # keep calm so that we can proceed processing
    except AttributeError:
        logger.error('Date might be invalid, please double check: %s', raw_date)
        return None


def safe_infer_volume(volume, movement):
    """Use context to garantee info relevance.

    Examples:
        >>> safe_infer_volume('foo', 'load')
        >>> safe_infer_volume(0, 'load')
        >>> safe_infer_volume('44', 'load')
        '44'
        >>> safe_infer_volume('44', 'discharge')
        '44'
        >>> safe_infer_volume(33, 'foobar')
        Traceback (most recent call last):
        ValueError: Invalid cargo movement: foobar

    """
    if not try_apply(volume, float, int):
        return None

    if movement not in MOVEMENT_MAPPING.values():
        raise ValueError(f'Invalid cargo movement: {movement}')

    return volume
