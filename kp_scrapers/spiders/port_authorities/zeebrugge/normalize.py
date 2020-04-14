import datetime as dt
import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

IRRELEVANT_VESSEL_TYPES = [
    'cc',  # container vessel
    'pr',  # passenger ferry
    'ps',  # passenger ferry
    'ro',  # roro cargo
    'rf',  # roro cargo
    'rv',  # vehicles carrier
    'vc',  # vehicles carrier
]
REPORTED_DATE_PATTERN = r'(\d{1,2}\/\d{1,2}\/\d{4}\s\d{1,2}:\d{1,2})'


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    # FIXME raw_item may sometimes not contain any data; we discard them
    if 'lblNaam' not in raw_item:
        return

    # map/normalize individual raw fields to something resembling a valid event
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)

    # discard irrelevant vessels
    _type = item.pop('vessel_type', '').strip()
    if _type in IRRELEVANT_VESSEL_TYPES:
        logger.info(f"Vessel {item['vessel_name']} is of an irrelevant type: {_type}")
        return
    if not item['vessel_imo'] or not item['vessel_imo'][0].startswith('9'):
        # source will always provide IMO for vessels that have it
        # if not, we should discard the vessel because it is too small and irrelevant
        logger.info(f"Vessel {item['vessel_name']} does not have an IMO number; irrelevant")
        return

    # build Vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'imo': item.pop('vessel_imo'),
        'length': item.pop('vessel_length'),
        'dead_weight': item.pop('vessel_dwt'),
        'gross_tonnage': item.pop('vessel_gt'),
        'call_sign': item.pop('vessel_callsign'),
        'flag_code': item.pop('vessel_flag'),
    }

    # build Cargoes sub-model
    item['cargoes'] = []
    for movement in ('load', 'discharge'):
        cargo = item.pop(f'cargo_{movement}')
        if cargo:
            item['cargoes'].append({'product': cargo, 'movement': movement})

    # build ETA
    eta = parse_date(f'{item.pop("eta_date")} {item.pop("eta_time")}', dayfirst=True)
    reported_date = parse_date(item['reported_date'], dayfirst=False)
    # discard if more than 3 months from reported date
    if eta - reported_date > dt.timedelta(days=90):
        logger.info(f"Forecasted PC for vessel {item['vessel']['name']} is too far in the future")
        return
    item['eta'] = eta.isoformat()

    return item


def portcall_mapping():
    # exhaustive mapping for development/debug clarity
    return {
        'lblAgent': ignore_key('shipping agent'),
        'lblBT': ('vessel_gt', None),
        'lblBemanning': ignore_key('crew number onboard'),
        'lblBestemming': ignore_key('TODO use next portcall'),
        'lblBestemmingETA': ignore_key('TODO use ETA of next portcall'),
        'lblBreedte': ignore_key('vessel beam; not needed for now'),
        'lblBunkeren': ignore_key('bunkering cargo'),
        'lblBunkerenTijdstip': ignore_key('time of bunkering'),
        'lblDWT': ('vessel_dwt', None),
        'lblDatumAankomst': ('eta_date', None),
        'lblDatumVertrek': ignore_key('estimated date of departure'),
        'lblDiepgang': ignore_key('vessel max draught'),
        'lblDiepgangAankomst': ignore_key('draught at arrival'),
        'lblDiepgangVertrek': ignore_key('draught at departure'),
        'lblDoelAanloop': ignore_key('purpose of previous portcall'),
        'lblDossiernummer': ignore_key('internal file number'),
        'lblHerkomst': ignore_key('previous portcall'),
        'lblHerkomstETS': ignore_key('departure date of previous portcall'),
        'lblIMONummer': ('vessel_imo', None),
        'lblLaden': ('cargo_load', None),
        'lblLadenTijdstip': ignore_key('time of cargo loading'),
        # length has decimal places with comma separators
        'lblLengte': ('vessel_length', lambda x: x.split(',')[0]),
        'lblLijndienst': ignore_key('previous portcall shipping agent ?'),
        'lblLossen': ('cargo_discharge', None),
        'lblLossenTijdstip': ignore_key('time of cargo discharging'),
        'lblNaam': ('vessel_name', None),
        'lblOpmerkingen': ignore_key('portcall remarks'),
        'lblPlaats': ignore_key('current location of vessel, at time of reporting'),
        'lblRederij': ignore_key('departure shipping agent'),
        'lblRoepnaam': ('vessel_callsign', None),
        'lblSlagzij': ignore_key('vessel heel'),
        'lblStatus': ignore_key('current status of the vessel'),
        'lblTijdstipAankomst': ('eta_time', None),
        'lblTijdstipVertrek': ignore_key('estimated time of departure'),
        'lblType': ('vessel_type', None),
        'lblVerblijfsnummer': ignore_key('internal portcall number'),
        'lblVlag': ('vessel_flag', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', normalize_reported_date),
    }


def normalize_reported_date(raw_date):
    match = re.search(REPORTED_DATE_PATTERN, raw_date)
    if match:
        return to_isoformat(match.group(1), dayfirst=True)

    raise ValueError(f"Unknown reported date format: {raw_date}")
