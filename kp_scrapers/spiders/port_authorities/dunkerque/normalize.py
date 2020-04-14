import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


CARGO_BLACKLIST = ['container', 'conteneur', 'coutainer', 'diver']

MOVEMENT_MAPPING = {'Chargement': 'load', 'Déchargement': 'discharge'}

INSTALLATION_MAPPING = {
    'arcelor': 'ArcelorMittal Dunkerque',
    'fland': 'Flandres',
    'rubi': 'Rubis Dunkirk',
}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:
    """
    item = map_keys(raw_item, key_mapping())
    # do not yield vessels with irrelevant cargoes
    if not item['cargoes']:
        logger.info(f'Vessel {item["vessel"]["name"]} does not contain relevant cargo')
        return

    return item


def key_mapping():
    return {
        'Berth': ('installation', lambda x: INSTALLATION_MAPPING.get(x.lower())),
        'Berth number': ignore_key('irrelevant'),
        'Destination': ignore_key('not required yet'),
        'Entry date': ('berthed', lambda x: to_isoformat(x, dayfirst=False, yearfirst=True)),
        'Expected berth': ('installation', None),
        'Forecast arrival': ('eta', lambda x: to_isoformat(x, dayfirst=False, yearfirst=True)),
        'Forecast departure': ignore_key('not required yet'),
        'From': ignore_key('not required yet'),
        'Name of vessel': ('vessel', lambda x: {'name': x}),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'Scheduled line': ignore_key('irrelevant'),
        'Shipping agent': ignore_key('irrelevant'),
        'Shipflow-paq_id': ignore_key('irrelevant'),
        'shipflow-operation': ('cargoes', lambda x: list(build_cargoes(x))),
    }


def build_cargoes(raw_cargo):
    """Build cargo movements from raw cargo string.

    Args:
        raw_cargo (str):

    Returns:
        List[Dict[str, str]]:
    """
    # some vessels don't provide cargo movements
    if not raw_cargo:
        return

    _cargoes = {k: v.split(',') for k, v in _clean_raw_cargo(raw_cargo).items()}
    # discard non-merchant vessels
    if 'Commerciale' not in _cargoes['type'][0]:
        return

    for _, movement, product, volume in zip(*_cargoes.values()):
        # do not yield irrelevant cargoes
        if any(alias in product.lower() for alias in CARGO_BLACKLIST):
            logger.info(f'Product is irrelevant: {product}')
            continue

        yield {
            'product': product,
            'movement': MOVEMENT_MAPPING.get(movement),
            'volume': volume.split()[0],
            # all relevant cargo volumes are given in tonnes by the website
            'volume_unit': Unit.tons,
        }


def _clean_raw_cargo(raw_cargo):
    """Converts hidden fields into a format much easier to parse

    Args:
        raw_cargo (str):

    Returns:
        List[tuple(str, str)]: Cleaner format for easier parsing downstream

    Examples:  # noqa
        >>> _clean_raw_cargo('Shipflow-operation-title Shipflow-type Commerciale Shipflow-nature Déchargement Shipflow-marchandise Charbon Shipflow-quantite 15000 Tonnes') # doctest: +NORMALIZE_WHITESPACE
        {'type': 'Commerciale',
         'nature': 'Déchargement',
         'marchandise': 'Charbon',
         'quantite': '15000 Tonnes'}
        >>> _clean_raw_cargo('Shipflow-operation-title Shipflow-type Commerciale Shipflow-nature Déchargement Shipflow-marchandise Iron Ore Shipflow-quantite 49392 Tonnes Shipflow-type Commerciale Shipflow-nature Déchargement Shipflow-marchandise Iron Ore Shipflow-quantite 126579 Tonnes') # doctest: +NORMALIZE_WHITESPACE
        {'type': 'Commerciale,Commerciale',
         'nature': 'Déchargement,Déchargement',
         'marchandise': 'Iron Ore,Iron Ore',
         'quantite': '49392 Tonnes,126579 Tonnes'}
        >>> _clean_raw_cargo('Shipflow-operation-title Shipflow-type Soutage Shipflow-nature Aucun Shipflow-type Soutage Shipflow-nature Aucun') # doctest: +NORMALIZE_WHITESPACE
        {'type': 'Soutage,Soutage', 'nature': 'Aucun,Aucu'}
    """
    _cargoes = [
        each.split(' ', 1)
        for each in may_strip(raw_cargo.strip('Shipflow-operation-title')).split('Shipflow-')[1:]
    ]

    res = {}
    for key, value in _cargoes:
        res[key] = res[key] + ',' + may_strip(value) if res.get(key) else may_strip(value)

    return res
