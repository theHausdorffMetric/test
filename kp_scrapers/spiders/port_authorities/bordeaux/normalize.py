import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Map and normalise raw item to a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # build vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'dwt': item.pop('vessel_dwt'),
        'gross_tonnage': item.pop('vessel_gross_tonnage', None),
        'length': item.pop('vessel_length', None),
    }

    # build cargoes sub-model
    item['cargoes'] = init_cargoes(
        discharge=item.pop('discharge', None), load=item.pop('load', None)
    )

    # discard vessel movements without cargoes
    if not item['cargoes']:
        logger.info(f'Vessel {raw_item["Vessel"]} has no cargo, discarding')
        return

    return item


def portcall_mapping():
    return {
        'Accostage': ('berthed', lambda x: to_isoformat(x, dayfirst=True)),
        'Agent': ignore_key('shipping agent'),
        'Agent montée': ignore_key('shipping agent in'),
        'Agent sortie': ignore_key('shipping agent out'),
        'Armateur': ignore_key('vessel owner'),
        'Chargement': ('load', lambda x: x[2:]),
        'Date prévue arrivée': ('eta', lambda x: to_isoformat(x, dayfirst=True)),
        'Déchargement': ('discharge', lambda x: x[2:]),
        'Destination': ignore_key('next port of call'),
        'Jauge brute': ('vessel_gross_tonnage', int),
        'Jauge nette': ignore_key('vessel net tonnage'),
        'Largeur': ignore_key('vessel beam/breadth'),
        'Longueur': (
            'vessel_length',
            lambda x: try_apply(x.replace('m', '').strip(), float, int, str),
        ),
        'Pavilion': ignore_key('berth'),
        'N° d\'escale': ignore_key('internal portcall designation'),
        'Port d\'attache': ignore_key('vessel flag city'),
        'Port en lourd': ('vessel_dwt', lambda x: try_apply(x, int, str)),
        'port_name': ('port_name', None),
        'Poste': ignore_key('station ?'),
        'Provenance': ignore_key('preceding port of call'),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'Tirant d\'eau AR montée': ignore_key('duplicated cargo field'),
        'Vessel': ('vessel_name', None),
    }


def init_cargoes(**cargoes):
    """Build Cargo from mapped item.

    Args:
        cargoes (Dict[str, str]): dict of {movement: product}

    Returns:
        Dict[str, str] | None:

    """
    res = []
    for movement, product in cargoes.items():
        if product:
            res.append({'product': product, 'movement': movement})

    return res if res else None
