from dateutil.parser import parse as parse_date

from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


IRRELEVANT_PRODUCT = 'CONTÊINERES'


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, field_mapping())

    if not item['cargoes']:
        return

    return item


def field_mapping():
    return {
        'Navio': ('vessel', lambda x: {'name': x}),
        'Nº Programação': ignore_key('number of schedule'),
        'DUV': ignore_key('DUV'),
        'ETA': ('eta', lambda x: parse_date(x, dayfirst=True) if x else None),
        'ARRIVAL': ('arrival', lambda x: parse_date(x, dayfirst=True) if x else None),
        'Serviço': ignore_key('service'),
        'Armador': ignore_key('owner'),
        'Agência': ignore_key('agency'),
        'Mercadoria': ('cargoes', lambda x: None if x == IRRELEVANT_PRODUCT else [{'product': x}]),
        'Confirmada Atracação': ignore_key('confirmed mooring'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: parse_date(x, fuzzy=True, dayfirst=True)),
    }
