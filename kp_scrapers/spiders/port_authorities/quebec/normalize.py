from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


VESSEL_NAME_BLACKLIST = ['A ETRE CONFIRME']

PRODUCT_BLACKLIST = [
    'Arrêt de nuit',
    'Attend les ordres',
    'Cargaison générale',
    'Changement de pavillon',
    'Compléter construction',
    'Croisière',
    'Dégivrage',
    'Désarmement',
    'EN SERVICE',
    'Hivernage',
    'Inspection',
    'Maintenance',
    'Nettoyage',
    'Passager transité',
    'Poste d\'attente',
    'Quai déjà occupé',
    'Ravitaillement',
    'Réparations',
    'Touage',
    'Visite',
]

CARGO_MOVEMENT_MAPPING = {'C': 'load', 'D': 'discharge'}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transfer raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:
    """
    item = map_keys(raw_item, field_mapping())
    # discard irrelevant vessels
    if not item['vessel_name'] or not item['cargo_product']:
        return

    # build Vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'length': item.pop('vessel_length', None),
        'dwt': item.pop('vessel_dwt', None),
        'gross_tonnage': item.pop('vessel_gt', None),
    }

    # build Cargo sub-model
    item['cargoes'] = [
        {'product': item.pop('cargo_product'), 'movement': item.pop('cargo_movement', None)}
    ]

    return item


def field_mapping():
    return {
        '0': ignore_key('berth'),
        '1': ('vessel_name', lambda x: None if x in VESSEL_NAME_BLACKLIST else x),
        '2': ignore_key('port-specific code'),
        '3': ('eta', lambda x: to_isoformat(x.replace('AM', '').replace('PM', ''), dayfirst=False)),
        '4': ignore_key('estimated docking duration'),
        '5': ignore_key('vessel flag'),
        '6': ('vessel_length', lambda x: x.partition(',')[0]),
        '7': ignore_key('arrival draught'),
        '8': ignore_key('departure draught'),
        '9': ('vessel_gt', lambda x: x.replace(',', '')),
        '10': ('vessel_dwt', lambda x: x.replace(',', '')),
        '11': ignore_key('shipping agent'),
        '12': ignore_key('previous port'),
        '13': ignore_key('next port'),
        '14': ('cargo_movement', lambda x: CARGO_MOVEMENT_MAPPING.get(x)),
        '15': (
            'cargo_product',
            lambda x: None if any(alias in x for alias in PRODUCT_BLACKLIST) else x,
        ),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }
