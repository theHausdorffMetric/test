import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.customs import CustomsPortCall
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.customs.united_states import constants


logger = logging.getLogger(__name__)


@validate_item(CustomsPortCall, normalize=True, strict=True, log_level='error')
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    # discard vessels without IMO number or name
    # source provides IMO numbers consistently, so if a vessel is listed without one,
    # it means the vessel is not registered with an IMO number, and thus too small for us
    if not item.get('vessel_imo') or not item.get('vessel_name'):
        logger.warning("Vessel has no name/IMO number, discarding: %s", item.get('vessel_name'))
        return

    # discard vessels with irrelevant type codes
    _type_code = item.pop('vessel_type', None)
    item['vessel_type'] = constants.VESSEL_TYPE_CODES.get(_type_code)
    if not item['vessel_type']:
        logger.debug("Vessel has irrelevant type: %s", _type_code)
        return

    # discard vessels calling at irrelevant ports
    if item.get('port_name') in constants.BLACKLISTED_PORTS:
        logger.debug("Portcall is at an irrelevant port: %s", item.get('port_name'))
        return

    # discard vessels with unknown purpose/movement
    if not item.get('purpose'):
        return

    # build Vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name', None),
        'imo': item.pop('vessel_imo', None),
        'call_sign': item.pop('vessel_callsign', None),
        'flag_code': item.pop('vessel_flag', None),
        'vessel_type': item.pop('vessel_type', None),
    }

    return item


def field_mapping():
    return {
        'Agent Name': ('shipping_agent', _clean_string),
        'Call Sign Number': ('vessel_callsign', _clean_string),
        'Draft': ('draught', _foot_to_metre),
        'Filing Date': (
            'filing_date',
            # dates are in MM/DD/YYYY format
            lambda x: to_isoformat(x, dayfirst=False, yearfirst=False),
        ),
        'IMO Number': ('vessel_imo', _clean_string),
        'Manifest Number': ('manifest', _clean_string),
        'Official Number': ignore_key('official number'),
        'Operator Name': ignore_key('vessel operator'),
        'Owner Name': ignore_key('vessel owner'),
        'Tonnage': ignore_key('TODO unsure if it describes cargo tonnage or vessel tonnage'),
        'Total Crew': ignore_key('total crew onboard vessel'),
        'Trade Code': ignore_key('unknown'),
        'Vessel Flag': ('vessel_flag', _clean_string),
        'Vessel Name': ('vessel_name', _clean_vessel_name),
        'Vessel Type Code': ('vessel_type', _clean_string),
        'Filing Port Code': ignore_key('filing port code'),
        'Filing Port Name': ('port_name', _clean_string),
        'Next Domestic Port': ('next_domestic_port', _clean_string),
        'Next Foreign Country': ('next_foreign_country', _clean_string),
        'Next Foreign Port Name': ('next_foreign_port', _clean_string),
        'PAX': ignore_key('number of passengers as payload; this should be zero anyway for us'),
        'Voyage Number': ignore_key('vessel voyage ID as recorded by filing port'),
        'Last Domestic Port': ('last_domestic_port', _clean_string),
        'Last Foreign Country': ('last_foreign_country', _clean_string),
        'Last Foreign Port': ('last_foreign_port', _clean_string),
        'Dock Name': ('dock_name', _clean_string),
        'Dock InTrans': ('purpose', lambda x: _normalize_purpose(may_strip(x))),
        'provider_name': ('provider_name', None),
        'source': ('source', None),
    }


def _foot_to_metre(value):
    """Convert foot to metre.

    TODO could be made generic

    Args:
        value (str): feet and inches - X'Y"

    Returns:
        int: value in metres

    Examples:
        >>> _foot_to_metre('27\\'6"')
        8.38
        >>> _foot_to_metre(None)

    """
    # sanity check
    if value == '' or value is None:
        return None

    # conversion factor
    FOOT_TO_METRE = 0.3048
    INCH_TO_METRE = 0.0254

    feet, inches = [float(e) for e in value.replace('"', '').split("'")]
    meters = feet * FOOT_TO_METRE + inches * INCH_TO_METRE
    # 2 decimal places is precise enough
    return round(meters, 2)


def _clean_vessel_name(name):
    """Clean raw vessel name.

    Provider may give a historical vessel name in the document.

    Examples:
        >>> _clean_vessel_name('AS FABIANA V2GT6 (EX: VLIET TRADER)')
        'AS FABIANA V2GT6'
        >>> _clean_vessel_name('East Coast, Former: NOREASTER')
        'East Coast'
        >>> _clean_vessel_name(None)

    """
    return name.split(',')[0].split('(')[0].strip() if name else None


def _normalize_purpose(raw):
    """Normalize vessel purpose as filed within customs authority.

    Vessel purposes are as stated in US Customs and Border Protection Form 1300, under
    Section 30 - PURPOSE OF ENTRANCE OR CLEARANCE
    See https://www.cbp.gov/sites/default/files/assets/documents/2018-Apr/CBP%20Form%201300.pdf

        D : Discharge Foreign Cargo
        X : Export Cargo Aboard on Arrival
        L : Lade Cargo for Export
        F : FROB - Foreign Cargo to Remain on Board
        N : No Cargo transactions
        Y : Military Cargo for Discharge/to be Laden

    L and D are top-priority purposes (L = load, D = discharge);
    set purpose to load/discharge whenever possible.

    Some combined purposes can be interpreted in terms of L & D:
        LX or X or FX ->> indicates load
        DX, DF or F ->> indicates discharge
        N or null ->> indicates no_movement
        DL is an error and should be returned as None

    Examples:
        >>> _normalize_purpose('D')
        'discharge'
        >>> _normalize_purpose('LX')
        'load'
        >>> _normalize_purpose('DFLX')
        >>> _normalize_purpose(None)

    """
    # sanity check
    if not raw:
        return None

    # if D and L are in the same indicated vessel movement, it is an error by the source
    if 'D' in raw and 'L' in raw:
        return None

    if raw in ['L', 'LX', 'X', 'FX']:
        return 'load'
    elif raw in ['D', 'DX', 'DF', 'F']:
        return 'discharge'
    elif 'N' in raw:
        return 'nothing'
    else:
        raise ValueError(f"Unknown USCustoms vessel purpose: {raw}")


def _clean_string(raw):
    """Clean strings and transform empty strings into NoneType.

    Examples:
        >>> _clean_string('  ')
        >>> _clean_string('')
        >>> _clean_string(' DOW CHEMICAL TEXAS OPERATIONS')
        'DOW CHEMICAL TEXAS OPERATIONS'
        >>> _clean_string(None)

    """
    if not raw:
        return None

    cleaned = may_strip(raw)
    return cleaned if cleaned else None
