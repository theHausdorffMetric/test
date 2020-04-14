import datetime as dt
import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


SPOT_CHARTER_STATUS_MAPPING = {
    'CORR': SpotCharterStatus.fully_fixed,
    'FLD': SpotCharterStatus.failed,
    'FXD': SpotCharterStatus.fully_fixed,
    'OOS FXD': SpotCharterStatus.fully_fixed,
    'RPLC': SpotCharterStatus.replaced,
    'SUB': SpotCharterStatus.on_subs,
    'TENDER': SpotCharterStatus.on_subs,
}

VOLUME_UNIT_MAPPING = {'KMT': Unit.kilotons}

# these are the values given by the source should a vessel be unknown
VESSEL_NAME_UNKNOWN = 'TBN'
VESSEL_IMO_UNKNOWN = '1111111'


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:
    """
    item = map_keys(raw_item, charters_mapping(), skip_missing=True)

    # discard unknown vessels
    if VESSEL_NAME_UNKNOWN in item['vessel_name'] or VESSEL_IMO_UNKNOWN in item['vessel_imo']:
        logger.info(f'Unknown vessel, discarding:\n{item}')
        return

    # build Vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'imo': item.pop('vessel_imo')}

    # build Cargo sub-model
    item['cargo'] = {
        'product': item.pop('cargo_product'),
        # spot charters are export movements by default
        'movement': 'load',
        'volume': item.pop('cargo_volume'),
        'volume_unit': item.pop('cargo_volume_unit'),
    }

    # NOTE: cargo information removed at the request of analysts
    # due to inaccuracy
    item.pop('cargo', None)

    # increment laycan_end by one day to improve matching rate
    item['lay_can_end'] = (parse_date(item['lay_can_end']) + dt.timedelta(days=1)).isoformat()

    return item


def charters_mapping():
    # declarative mapping for ease of maintenance
    return {
        'cargo': ('cargo_product', None),
        'cargo_id': ignore_key('irrelevant'),
        'cargo_size': ('cargo_volume', None),
        'cargo_type_id': ignore_key('irrelevant'),
        'cargo_unit': ('cargo_volume_unit', map_volume_unit),
        'cargo_value_mt': ignore_key('irrelevant'),
        'cargo_value_total': ignore_key('irrelevant'),
        'charter_type': ignore_key('irrelevant'),
        'charterer': ('charterer', None),
        'correctness': ignore_key('reliability of data ? TBC with analysts'),
        'delivery_port': ignore_key('irrelevant'),
        'discharge_country': ignore_key('alternative to `arrival_zone`'),
        'discharge_country_id': ignore_key('irrelevant'),
        'discharge_port': ('arrival_zone', lambda x: x.split('-')),
        'discharge_port_id': ignore_key('irrelevant'),
        'discharge_zone': ignore_key('alternative to `arrival_zone`'),
        'discharge_zone_id': ignore_key('irrelevant'),
        'dwt': ignore_key('vessel deadweight tonnage'),
        'freight_price': ignore_key('irrelevant'),
        'insertion_datetime': (
            'reported_date',
            lambda x: parse_date(x, dayfirst=False).strftime('%d %b %Y'),
        ),
        'laycan_start': ('lay_can_start', lambda x: to_isoformat(x, dayfirst=False)),
        'laycan_end': ('lay_can_end', lambda x: to_isoformat(x, dayfirst=False)),
        'load_country': ignore_key('alternative to `departure_zone`'),
        'load_country_id': ignore_key('irrelevant'),
        'load_port': ('departure_zone', None),
        'load_port_id': ignore_key('irrelevant'),
        'load_zone': ignore_key('alternative to `departure_zone`'),
        'load_zone_id': ignore_key('irrelevant'),
        'loadport_id_vertified_ais': ignore_key('irrelevant'),
        'loadport_vertified_ais': ignore_key('irrelevant'),
        'owner': ignore_key('vessel owner'),
        'period_max': ignore_key('irrelevant'),
        'period_min': ignore_key('irrelevant'),
        'provider_name': ('provider_name', None),
        'rate': ('rate_value', None),
        'redelivery_port': ignore_key('irrelevant'),
        'source': ignore_key('source of the data ? TBC with analysts'),
        'status': ('status', map_spot_charter_status),
        'terms': ignore_key('irrelevant'),
        'updated_date': ignore_key('possible alternative as `reported_date` ?'),
        'verified': ignore_key('confirmation of data correctness by source ? TBC with analysts'),
        'vessel_fixture_id': ignore_key('irrelevant'),
        'vessel_imo': ('vessel_imo', lambda x: try_apply(x, str)),
        'vessel_name': ('vessel_name', None),
        'vessel_type': ignore_key('vessel size category'),
        'vessel_type_id': ignore_key('irrelevant'),
        'year_built': ignore_key('vessel build year'),
    }


def map_volume_unit(raw_unit):
    return _map_raw_to_enum(raw_unit, volume_unit=VOLUME_UNIT_MAPPING)


def map_spot_charter_status(raw_status):
    return _map_raw_to_enum(raw_status, charter_status=SPOT_CHARTER_STATUS_MAPPING)


def _map_raw_to_enum(raw, **_mapping):
    """Map a raw string to an existing enum whilst logging and conducting sanity checks.

    TODO could be made generic ?

    Args:
        raw (str): raw string from source
        **mapping (Dict[str, Dict[str, Any]]): mapping of raw string to an enum; this should only
                                               contain one string

    Returns:
        Optional[str]:
    """
    # NOTE kwarg of `mapping` is only for hiding implementation detail
    # of the log message needing a namespace; there should only be one mapping.
    if len(_mapping) > 1:
        raise ValueError("There should only be one mapping.")

    # discard missing raws
    if not raw or raw == '-':
        return None

    name, mapping = next(iter(_mapping.items()))
    mapped = mapping.get(raw)
    if not mapped:
        logger.warning(f'Unknown {name}: {raw}')

    return mapped
