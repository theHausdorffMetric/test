import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_remove_substring, may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

MOVEMENT_MAPPING = {'Imbarco': 'load', 'Sbarco': 'discharge'}

DATESTRING_BLACKLIST = [',', '.', 'AM', 'PM', 'ALBA', 'SERA']


@validate_item(PortCall, normalize=True, strict=True)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Yields:
        ArrivedEvent:
        EtaEvent:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    # build vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'imo': item.pop('vessel_imo', None),
        'length': item.pop('vessel_length', None),
        'gross_tonnage': item.pop('vessel_gross_tonnage', None),
    }

    # According to the source, even there are multiple products the reciever will be the same
    # for each vessel. This is how the source displays the results.
    if item.get('buyer'):
        for cargo in item.get('cargoes'):
            cargo['buyer'] = {'name': item.get('buyer')}

    item.pop('buyer', None)

    return item


def field_mapping():
    return {
        'Agency:': ignore_key('shipping agent'),
        'Berth:': ignore_key('shipping agent'),
        'Data arrivo in rada': ('arrival', normalize_date),
        'cargoes': ('cargoes', lambda x: [build_cargo(cargo) for cargo in x]),
        'Draft:': ignore_key('vessel draught'),
        'ETA': ('eta', normalize_date),
        'EDB:': ignore_key('estimated date of berthing'),
        'Flag:': ignore_key('vessel flag'),
        'GRT:': ('vessel_gross_tonnage', lambda x: x.replace(',', '')),
        'IMO:': ('vessel_imo', None),
        'Length:': ('vessel_length', lambda x: try_apply(x.replace(',', ''), float, int, str)),
        'provider_name': ('provider_name', may_strip),
        'port_name': ('port_name', may_strip),
        'Receiver:': ('buyer', None),
        'reported_date': ('reported_date', None),
        'S.I.:': ignore_key('unknown'),
        'Ship:': ('vessel_name', may_strip),
    }


def build_cargo(cargo):
    """Normalize cargo information for each vessel

    Args:
        cargoes (List[Dict[str, str]]): list of cargo dicts

    Returns:
        List[Dict[str, str]]:

    """
    return {
        'product': cargo['product'],
        'movement': MOVEMENT_MAPPING.get(cargo['movement']),
        'volume': cargo['volume'].replace(',', '') if cargo['volume'] else None,
        'volume_unit': Unit.tons,
    }


def normalize_date(date_str):
    """Normalize date information to ISO 8601 format

    Args:
        date_str (str): raw, fuzzy date string

    Returns:
        str:

    """
    return to_isoformat(may_remove_substring(date_str, blacklist=DATESTRING_BLACKLIST))
