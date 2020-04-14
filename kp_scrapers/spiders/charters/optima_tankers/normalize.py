import logging
from typing import Any, Dict, Tuple

from kp_scrapers.lib.date import get_date_range
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


STATUS_MAPPING = {
    'FLD': SpotCharterStatus.failed,
    'FAILED': SpotCharterStatus.failed,
    'FAIL': SpotCharterStatus.failed,
}

logger = logging.getLogger(__name__)

MISSING_ROWS = []


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    item = map_keys(raw_item, field_mapping(), skip_missing=True)
    if item.get('vessel_status'):
        vessel_name, item['status'] = normalize_vessel_status(item.pop('vessel_status'))
        item['vessel'] = {'name': vessel_name}

    if item.get('charterer_status'):
        item['charterer'], item['status'] = normalize_charterer_status(item.pop('charterer_status'))

    if not item['vessel']['name']:
        return

    try:
        item['lay_can_start'], item['lay_can_end'] = get_date_range(
            item.pop('lay_can'), '/', '-', item['reported_date']
        )
    except Exception:
        MISSING_ROWS.append(str(raw_item))
        return
    item['departure_zone'], item['arrival_zone'] = normalize_voyage(item.pop('voyage'))

    # since dpp attachment contains no specific product, we map it to the tree
    if 'dpp' in item['mail_title'].lower():
        product = 'Dirty'
    if 'cln' in item['mail_title'].lower():
        product = item.pop('cargo_product')
    item.pop('mail_title')

    # build cargo sub-model
    item['cargo'] = {
        'product': product,
        'volume': item.pop('cargo_volume'),
        'volume_unit': Unit.kilotons,
        'movement': 'load',
    }

    return item


def field_mapping() -> Dict[str, tuple]:
    return {
        'vessel_status': ('vessel_status', None),
        'vessel': ('vessel', lambda x: {'name': x if x and 'TBN' not in x else None}),
        'cargo_volume': ('cargo_volume', None),
        'cargo_product': ('cargo_product', None),
        'lay_can': ('lay_can', None),
        'voyage': ('voyage', None),
        'rate_value': ('rate_value', None),
        'charterer': ('charterer', None),
        'charterer_status': ('charterer_status', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'mail_title': ('mail_title', None),
    }


def normalize_vessel_status(raw_vessel_status: str) -> Tuple[str]:
    vessel_status_tuple = raw_vessel_status.replace(')', '').partition('(')
    return (
        vessel_status_tuple[0] if 'TBN' not in vessel_status_tuple[0] else None,
        STATUS_MAPPING.get(vessel_status_tuple[-1], None),
    )


def normalize_charterer_status(raw_charterer_status: str) -> Tuple[str]:
    charter_status_tuple = raw_charterer_status.partition('-')
    return (
        may_strip(charter_status_tuple[0]),
        STATUS_MAPPING.get(may_strip(charter_status_tuple[-1]), None),
    )


def normalize_voyage(raw_voyage: str) -> Tuple[str, list]:
    departure_zones, _, arrival_zone = raw_voyage.partition('/')
    departure_zone, _, another = departure_zones.partition('+')

    if not arrival_zone or 'OPTS' in arrival_zone:
        return departure_zone, None

    return departure_zone, arrival_zone.replace('–', '-').split('-')
