import logging
from typing import Any, Callable, Dict, Optional, Tuple

from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.utils import validate_item
from kp_scrapers.models.vessel import VesselRegistry


logger = logging.getLogger(__name__)


@validate_item(VesselRegistry, normalize=True, strict=True, log_level='warning')
def process_item(raw_item: Dict[str, str]) -> Dict[str, Optional[str]]:
    """Transform raw item into usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    return map_keys(raw_item, vessel_mapping())


def vessel_mapping() -> Dict[str, Tuple[str, Callable[[str], Any]]]:
    # NOTE we deliberately choose to yield only MMSI and callsign values because all other sources
    # that we have don't provide these two values, and we
    # we don't want to override other vessel atrtibutes with MarineTraffic's less reliable updates
    return {
        'CALLSIGN': ('call_sign', _clean_void_string),
        'CODE2': ignore_key('vesse; flag code'),
        'COUNT_PHOTOS': ignore_key('number of photos the source has on the vessel'),
        'COUNTRY': ignore_key('vessel flag name'),
        'COURSE': ignore_key('current course of vessel'),
        'CTA_ROUTE_FORECAST': ignore_key('unknown'),
        'CURRENT_PORT': ignore_key('previous port name'),
        'DESTINATION': ignore_key('destination port'),
        'DISTANCE_TO_GO': ignore_key('how long more to go in nautical miles'),
        'ETA': ignore_key('eta to destination'),
        'ETA_OFFSET': ignore_key('timezone offset'),
        'ETA_UPDATED': ignore_key('last updated timestamp of eta'),
        'IMO': ('imo', None),
        'LAST_POS': ignore_key('latest position in unix time'),
        'LAT': ignore_key('latitude of vessel at latest position'),
        'LON': ignore_key('longitude of vessel at latest position'),
        'MMSI': ('mmsi', _clean_void_string),
        'NEXT_PORT_COUNTRY': ignore_key('destination port country'),
        'NEXT_PORT_ID': ignore_key('destination port internal id'),
        'NEXT_PORT_NAME': ignore_key('destination port name'),
        'PORT_ID': ignore_key('previous port internal id'),
        'provider_name': ('provider_name', None),
        'SHIP_ID': ignore_key('current vessel internal id'),
        'SHIPNAME': ignore_key('current vessel name'),
        'SPEED': ignore_key('speed of vessel in knots'),
        'TIMEZONE': ignore_key('timezone of latest position of vessel'),
        'TYPE_COLOR': ignore_key('unknown'),
        'TYPE_SUMMARY': ignore_key('vessel cargo classfication'),
    }


def _clean_void_string(string: str) -> Optional[str]:
    return None if string.startswith('-') else string
