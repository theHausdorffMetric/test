import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


IRRELEVANT_PORTCALL_TYPES = (
    '觀光',  # passengers
    '載客',  # passengers
    '加油',  # refuelling
    '修護',  # repair
)

IRRELEVANT_VESSEL_TYPES = ('客輪', '客貨輪', '貨櫃輪')  # ferry  # ferry  # containers


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, portcall_mapping())

    # discard items with irrelevant vessel type or portcall purpose
    _event, _vessel_type = item.pop('event'), item.pop('vessel_type')
    if _event in IRRELEVANT_PORTCALL_TYPES:
        logger.info(f'Portcall is of an irrelevant purpose, discarding: {_event}')
        return
    if _vessel_type in IRRELEVANT_VESSEL_TYPES:
        logger.info(f'Vessel is of an irrelevant type, discarding: {_vessel_type}')
        return

    # build Vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'call_sign': item.pop('vessel_callsign'),
        'gross_tonnage': item.pop('vessel_gross_tonnage'),
        'length': item.pop('vessel_length'),
    }

    return item


def portcall_mapping():
    return {
        'Aft.Dft': ignore_key('TODO draught at departure'),
        'Agent': ignore_key('shipping agent'),
        'Arl.Date Time': ('eta', lambda x: to_isoformat(x, dayfirst=False)),
        'Berth #': ignore_key('berth number; sometimes not present'),
        'Cat.': ('vessel_type', None),
        'Dep.Date Time': ignore_key('TODO departure date'),
        'Fnt.Dft': ignore_key('TODO draught at arrival'),
        'From': ignore_key('previous vessel port of call'),
        'G.R.T.': ('vessel_gross_tonnage', lambda x: str(round(float(x.replace(',', ''))))),
        'L.O.A.': ('vessel_length', lambda x: str(round(float(x)))),
        'Nationality': ignore_key('TODO vessel flag'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'Purpose': ('event', None),
        'reported_date': ('reported_date', None),
        'ShipName': ('vessel_name', None),
        'Signal': ('vessel_callsign', None),
        'To': ignore_key('TODO next vessel port of call'),
        'Voyage #': ignore_key('internal voyage ID'),
    }
