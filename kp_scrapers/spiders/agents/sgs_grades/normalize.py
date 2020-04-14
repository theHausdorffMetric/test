import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import CargoMovement
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


UNIT_MAPPING = {
    'barrels': Unit.barrel,
    'cubic metre': Unit.cubic_meter,
    'kilogram air': Unit.kilogram,
    'metric ton air': Unit.tons,
    'metric ton vacuum': Unit.tons,
}

MOVEMENT_MAPPING = {'discharge': 'discharge', 'loading': 'load'}


@validate_item(CargoMovement, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]: normalized cargo movement item

    """
    item = map_keys(raw_item, field_mapping())

    # discard vessel movements that contain possibly inaccurate products
    if not item.pop('is_actual', False):
        logger.info(f'Discarding {item["vessel_name"]} cargo movement as it may be inaccurate')
        return

    # build proper Vessel model
    item['vessel'] = {
        'name': item.pop('vessel_name', None),
        'imo': item.pop('vessel_imo', None),
        'dwt': item.pop('vessel_dwt', None),
    }

    # take parent product string if detailed product string is missing/invalid
    _product = item.pop('cargo_product_parent', None)
    _product_child = item.pop('cargo_product_child', None)
    item['cargo_product'] = _product_child if _product_child else _product

    # discard vessel movements that do not contain cargo
    if not item['cargo_product']:
        logger.info(f'Discarding {item["vessel"]["name"]} cargo movement as there is no cargo')
        return

    # handle additional units
    item['cargo_volume'], item['cargo_volume_unit'] = normalize_volume_unit(
        item.get('cargo_volume'), item.get('cargo_volume_unit')
    )

    # build proper Cargo model
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': item.pop('cargo_movement', None),
        'volume': item.pop('cargo_volume', None),
        'volume_unit': item.pop('cargo_volume_unit', None),
    }

    return item


def field_mapping():
    # declarative mapping for ease of developement/maintenance
    return {
        'api': ignore_key('irrelevant'),
        'arrivedDate': ('arrival', lambda x: to_isoformat(x, dayfirst=True)),
        'berthedDate': ('berthed', lambda x: to_isoformat(x, dayfirst=True)),
        'berthNumber': ignore_key('berth number of installation called at'),
        'blDate': ignore_key('bill of lading date'),
        'country': ignore_key('country of port call'),
        'countrycode': ignore_key('2-letter country designation of port call'),
        'density15': ignore_key('irrelevant'),
        'density20': ignore_key('irrelevant'),
        'destinationCountry': ignore_key('next port call country'),
        'destinationLocation': ignore_key('next port of call'),
        'dueDate': ('eta', lambda x: to_isoformat(x, dayfirst=True)),
        'ExportId': ignore_key('irrelevant'),
        'grade': ('cargo_product_child', lambda x: x if 'TBC' not in x else None),
        'id': ignore_key('irrelevant'),
        'ihsBerthId': ignore_key('irrelevant'),
        'ihsDestinationId': ignore_key('irrelevant'),
        'ihsLocationId': ignore_key('irrelevant'),
        'ihsTerminalId': ignore_key('irrelevant'),
        'imo': ('vessel_imo', lambda x: try_apply(x, int, str) if str(x) != '0' else None),
        'ishBerthId': ignore_key('irrelevant'),
        'lastUpdated': ignore_key('possible alternative for reported date'),
        'Location': ('port_name', None),
        'mmsi': ignore_key('vessel mmsi'),
        'operation': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x.lower())),
        'product': ('cargo_product_parent', None),
        'ProductStatus': ('is_actual', lambda x: 'actual' in x.lower()),
        'ProductStatusLastUpdated': (ignore_key('irrelevant')),
        'provider_name': ('provider_name', None),
        'quantity': ('cargo_volume', lambda x: try_apply(x, int, str) if str(x) != '0' else None),
        'QuantityStatus': ignore_key('irrelevant'),
        'QuantityStatusLastUpdated': ignore_key('irrelevant'),
        'reported_date': ('reported_date', None),
        'sailedDate': ('departure', lambda x: to_isoformat(x, dayfirst=True)),
        'sdwt': ('vessel_dwt', lambda x: try_apply(x, int, str) if str(x) != '0' else None),
        'sgsBerthid': ignore_key('irrelevant'),
        'sgsDestinationLocationId': ignore_key('irrelevant'),
        'sgsLocationId': ignore_key('irrelevant'),
        'sgsTerminalId': ignore_key('irrelevant'),
        'status': ignore_key('irrelevant'),
        'terminal': ignore_key('installation of port call; may be useful'),
        'unit': ('cargo_volume_unit', lambda x: x.lower()),
        'vessel': ('vessel_name', None),
    }


def normalize_volume_unit(raw_volume, raw_unit):
    """Normalize volume and volume units to a quantity that will be recognised by the ETL.

    Args:
        raw_volume (str | None):
        raw_unit (str):

    Returns:
        Tuple[str, str]: tuple of (volume, unit)

    Examples:
        >>> normalize_volume_unit(None, 'metric ton air')
        (None, None)
        >>> normalize_volume_unit('176000', 'pound')
        ('88', 'tons')
        >>> normalize_volume_unit('210000', 'us gallons')
        ('794', 'tons')
        >>> normalize_volume_unit('143000', 'barrels')
        ('143000', 'barrel')
    """
    if not raw_volume:
        return None, None

    # these two units are not supported by the ETL
    if raw_unit == 'pound':
        return str(int(float(raw_volume) * 0.0005)), 'tons'
    if raw_unit == 'us gallons':
        return str(int(float(raw_volume) * (1 / 264.17))), 'tons'

    return str(int(raw_volume)), UNIT_MAPPING.get(raw_unit)
