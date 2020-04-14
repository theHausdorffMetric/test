from json import JSONDecodeError
import logging

from dateutil.parser import parse as parse_date
from googletrans import Translator

from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

BLACKLIST = ['CONTAINERS']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        Dict:

    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # build cargo sub model
    cargoes = []
    disch = item.pop('cargo_discharge', None)
    if disch:
        cargoes.append(disch)
    load = item.pop('cargo_load', None)
    if load:
        cargoes.append(load)

    item['cargoes'] = cargoes

    return item


def portcall_mapping():
    return {
        "VESSEL'S NAME": ('vessel', lambda x: {'name': x}),
        'FLIGHT NUMBER': (ignore_key('ignore flight number')),
        'AGENT': ('shipping_agent', normalize_agent),
        'UNLOADING GOODS': ('cargo_discharge', lambda x: normalize_cargo(x, 'discharge')),
        'CARGO CARGO': ('cargo_load', lambda x: normalize_cargo(x, 'load')),
        'SHIPPING COMPANY': (ignore_key('shipping company')),
        'DATE OF ARRIVAL HIJACKER': ('arrival', lambda x: parse_date(x, fuzzy=True).isoformat()),
        'SUBMERSIBLE': (ignore_key('unknown')),
        'SIDEWALK': (ignore_key('unknown')),
        # meta info
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_cargo(raw_product, movement):
    """Translate raw product and build cargo item.

    Googletrans module actually calls google service, it has request limie. If we do this action
    frequently, we might encounter a JSONDecodeError.

    Args:
        raw_product: product in Arabic
        movement: load or discharge

    Returns:
        str: original and translated item with product info

    """
    if not raw_product:
        return
    p = '{} ({})'
    translator = Translator()
    try:
        product = translator.translate(raw_product).text.upper()
        if product not in BLACKLIST:
            return {'product': product, 'movement': movement}

    except JSONDecodeError:
        logger.warning('Googletrans API request limit')
        return {'product': p.format(raw_product, ''), 'movement': movement}


def normalize_agent(raw_agent):
    """Translate raw agent info.

    Args:
        raw_agent: agent in Arabic

    Returns:
        str: original and translated text

    """
    if not raw_agent:
        return

    p = '{} ({})'
    translator = Translator()
    try:
        agent = translator.translate(raw_agent).text
        return p.format(raw_agent, agent)
    except JSONDecodeError:
        logger.warning('Googletrans API request limit when translating agent field')
        return p.format(raw_agent, '')
