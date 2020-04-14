from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


PRODUCTS_TO_IGNORE = [
    'passengers',
    'cars',
    'cts',
    'imo',
    'autos',
    'passagers',
    'changement',
    'ballast',
]
NOISY_EXP = ['exp.', 'exp', 'imp.', 'imp']


@validate_item(PortCall, normalize=True, strict=True, log_level='error')
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, portcall_mapping())

    # discard portcall if no relevant portcall date found
    if not (item.get('eta') or item.get('arrival') or item.get('berthed')):
        return

    # discard irrelevant products
    if not item['cargoes']:
        return

    return item


def portcall_mapping():
    return {
        'N° Escale': ignore_key('internal stop number'),
        'Nom Navire': ('vessel', lambda x: {'name': may_strip(x.replace('*', ''))}),
        'Marchandise': ('cargoes', lambda x: normalize_cargo(x)),
        'D-Arrivée': ('arrival', to_isoformat),
        'D-Accostage': ('berthed', to_isoformat),
        'D-Sortie': ignore_key('no need to scrape estimated time of departure'),
        'Quai': ignore_key('berth is not needed since there is only one installation'),
        'Observation': ignore_key('comments by port authority'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'DUMMY': ignore_key('just a temporary header column to match the empty values'),
    }


def normalize_cargo(raw_cargo_str):
    cargoes = []
    for product in [may_strip(prod) for prod in raw_cargo_str.split('+')]:
        for nexp in NOISY_EXP:
            product = may_strip(product.lower().replace(nexp, ''))

        if any(PTI in product.lower() for PTI in PRODUCTS_TO_IGNORE):
            return None
        else:
            cargoes.append({'product': product})

    return cargoes if cargoes else None
