import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys, remove_diacritics
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MOVEMENT_MAPPING = {'ATRACACAO': '', 'DESC': 'discharge', 'EMB': 'load'}
PRODUCT_BLACKLIST = [
    'CELULOSE',
    'CONTEINERES',
    'CONSUMO',
    'GERAL',
    'MERCADORIAS',
    'OUTROS',
    'PAPEL',
    'RO-RO',
    'VEICULO',
]


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable item.

    Also dispatches raw item to their respective normalising functions.

    Args:
        raw_item (dict[str, str]):

    Returns:
        EtaEvent | BerthedEvent:

    """
    # dispatch to corresponding event processors
    if 'scheduled' in raw_item['url']:
        return process_eta_item(map_keys(raw_item, eta_mapping()))
    elif 'expected' in raw_item['url']:
        return process_arrival_item(map_keys(raw_item, arrival_mapping()))
    elif 'berthed' in raw_item['url']:
        return process_berthed_item(map_keys(raw_item, berthed_mapping()))


def eta_mapping():
    return {
        '0': ('eta_date', None),
        '1': ('eta_time', lambda x: x.split('/')[0]),
        '2': ignore_key('irrelevant place'),
        '3': ('vessel_name', remove_diacritics),
        '4': ('cargoes', lambda x: [{'product': x}]),
        '5': ignore_key('irrelevant event'),
        '6': ignore_key('irrelevant voyage'),
        '7': ignore_key('irrelevant duv'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
        'url': ignore_key('url no longer needed'),
    }


def process_eta_item(item):
    """Transform mapped item into a usable event.

    This function assumes item's url is 'http://www.portodesantos.com.br/esperados.php'
    and contains ETA (long-term) table rows.

    Args:
        item (dict[str, str]): mapped item

    Returns:
        EtaEvent:

    """
    item['vessel'] = {'name': item.pop('vessel_name')}
    item['eta'] = to_isoformat(f'{item.pop("eta_date")} {item.pop("eta_time")}')

    # discard irrelevant cargo
    if any(alias in item['cargoes'][0]['product'] for alias in PRODUCT_BLACKLIST):
        logger.warning(
            'Vessel {} is carrying irrelevant cargo: {}'.format(
                item['vessel']['name'], item['cargoes'][0]['product']
            )
        )
        return

    return item


def normalize_cargo(cargoes):
    for product, movement, volume in cargoes:
        print(repr(product), repr(movement), repr(volume))
        if not any(alias in product for alias in PRODUCT_BLACKLIST):
            yield {
                'product': product,
                # TODO confirm with analysts if we need the volumes
                'movement': MOVEMENT_MAPPING.get(movement.strip(), None),
                'volume': volume,
                'volume_unit': Unit.tons,
            }
        else:
            logger.warning(f'Irrelevant cargo: {product}')


def arrival_mapping():
    return {
        '0': ('vessel_name', remove_diacritics),
        '1': ignore_key('vessel flag is not needed for now'),
        '2': ('vessel_length', lambda x: x.split('<br>')[0]),
        '3': ignore_key('irrelevant'),
        '4': ('eta', lambda x: to_isoformat(x.replace('<br>', ' '), dayfirst=True)),
        '5': ignore_key('irrelevant'),
        '6': ignore_key('irrelevant'),
        '7': ('movements', lambda x: [i for i in x.split('<br>') if i]),
        '8': ('products', lambda x: [i for i in x.split('<br>') if i]),
        '9': ('volumes', lambda x: [i for i in x.split('<br>') if i]),
        '10': ignore_key('irrelevant'),
        '11': ignore_key('irrelevant'),
        '12': ignore_key('irrelevant'),
        '13': ('installation', lambda x: x.title() if x else None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
        'url': ignore_key('url no longer needed'),
    }


def process_arrival_item(item):
    """Transform mapped item into a usable event.

    This function assumes item's url is 'http://www.portodesantos.com.br/programados.php'
    and contains ETA table rows.

    Args:
        item (dict[str, str]): mapped item

    Returns:
        EtaEvent:

    """
    item['vessel'] = {'name': item.pop('vessel_name'), 'length': item.pop('vessel_length', None)}

    item['cargoes'] = list(
        normalize_cargo(zip(item.pop('products'), item.pop('movements'), item.pop('volumes')))
    )

    # discard vessels with irrelevant cargo
    if not item['cargoes'] or item['cargoes'] == [None]:
        return

    return item


def berthed_mapping():
    return {
        '0': ('berth', None),
        '1': ('vessel_name', remove_diacritics),
        '2': ignore_key('morning'),
        '3': ignore_key('afternoon'),
        '4': ignore_key('evening'),
        '5': ignore_key('night'),
        '6': ('cargo', None),
        '7': ('discharge', None),
        '8': ('load', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
        'url': ignore_key('url no longer needed'),
    }


def process_berthed_item(item):
    item['vessel'] = {'name': item.pop('vessel_name')}
    cargo = item.pop('cargo', None)
    if item.get('discharge') != '0':
        movement = 'discharge'
        volume = item.pop('discharge', None)
    elif item.get('load') != '0':
        movement = 'load'
        volume = item.pop('load', None)
    else:
        movement, volume = None, None

    if not cargo:
        return
    item['cargoes'] = [
        {
            'product': cargo,
            'movement': movement,
            'volume': volume,
            'volume_unit': Unit.tons if volume else None,
        }
    ]

    return item
