from kp_scrapers.lib.date import get_date_range
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        SpotCharter | None

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)
    if not item['vessel']:
        return

    # build cargo model
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'movement': 'load',
        'volume': item.pop('cargo_volume', None),
        'volume_unit': Unit.kilotons,
    }

    if not item['cargo']['product']:
        item.pop('cargo')

    item['lay_can_start'], item['lay_can_end'] = get_date_range(
        item.pop('lay_can'), '/', '-', item['reported_date']
    )
    item['departure_zone'], item['arrival_zone'] = normalize_voyage(item.pop('voyage'))

    return item


def field_mapping():
    return {
        '0': ('vessel', normalize_vessel),
        '1': ('cargo_volume', None),
        '2': ('cargo_product', None),
        '3': ('lay_can', None),
        '4': ('voyage', None),
        '5': ('rate_value', None),
        '6': ('charterer', normalize_charterer),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_vessel(raw_vessel_name):
    """Filter irrelevant vessels.

    Args:
        raw_vessel_name (str):

    Returns:
        str | None:

    """
    return None if 'TBN' in raw_vessel_name.split() else {'name': raw_vessel_name}


def normalize_voyage(raw_voyage):
    """Normalize departure zones and arrival zones.

    Examples:
        >>> normalize_voyage('E.GUINEA/CHINA')
        ('E.GUINEA', ['CHINA'])
        >>> normalize_voyage('NIGERIA//WC.INDIA')
        ('NIGERIA', ['WC.INDIA'])
        >>> normalize_voyage('NIGERIA//ARA-WC.INDIA')
        ('NIGERIA', ['ARA', 'WC.INDIA'])

    Args:
        raw_voyage (str):

    Returns:
        Tuple(List[str], str):

    """
    departure_zone, _, arrival_zone = raw_voyage.partition('/')
    return departure_zone, arrival_zone.replace('/', '').split('-')


def normalize_charterer(raw_charterer):
    """Replace ? as CNR.

    Args:
        raw_charterer (str):

    Returns:
        str:

    """
    return None if '?' in raw_charterer else raw_charterer
