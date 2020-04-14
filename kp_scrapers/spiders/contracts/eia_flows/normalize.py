from collections import namedtuple
import logging

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import FR, relativedelta, SA

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys


logger = logging.getLogger(__name__)

Flow = namedtuple('Flow', ('origin', 'destination'))
SERIES_FLOW_MAPPING = {
    # Weekly U.S. Exports of Crude Oil
    'PET.WCREXUS2.W': Flow(origin='United States', destination=None),
    # Weekly U.S. Commercial Crude Oil Imports Excluding SPR
    'PET.WCRIMUS2.W': Flow(origin=None, destination='United States'),
    # Weekly East Coast (PADD 1) Commercial Crude Oil Imports Excluding SPR
    'PET.WCEIMP12.W': Flow(origin=None, destination='PADD 1'),
    # Weekly Gulf Coast (PADD 3) Commercial Crude Oil Imports Excluding SPR
    'PET.WCEIMP32.W': Flow(origin=None, destination='PADD 3'),
    # Weekly West Coast (PADD 5) Commercial Crude Oil Imports Excluding SPR
    'PET.WCEIMP52.W': Flow(origin=None, destination='PADD 5'),
    # Weekly U.S. Imports from Brazil of Crude Oil
    'PET.W_EPC0_IM0_NUS-NBR_MBBLD.W': Flow(origin='Brazil', destination='United States'),
    # Weekly U.S. Imports from Canada of Crude Oil
    'PET.W_EPC0_IM0_NUS-NCA_MBBLD.W': Flow(origin='Canada', destination='United States'),
    # Weekly U.S. Imports from Colombia of Crude Oil
    'PET.W_EPC0_IM0_NUS-NCO_MBBLD.W': Flow(origin='Colombia', destination='United States'),
    # Weekly U.S. Imports from Ecuador of Crude Oil
    'PET.W_EPC0_IM0_NUS-NEC_MBBLD.W': Flow(origin='Ecuador', destination='United States'),
    # Weekly U.S. Imports from Iraq of Crude Oil
    'PET.W_EPC0_IM0_NUS-NIZ_MBBLD.W': Flow(origin='Iraq', destination='United States'),
    # Weekly U.S. Imports from Kuwait of Crude Oil
    'PET.W_EPC0_IM0_NUS-NKU_MBBLD.W': Flow(origin='Kuwait', destination='United States'),
    # Weekly U.S. Imports from Mexico of Crude Oil
    'PET.W_EPC0_IM0_NUS-NMX_MBBLD.W': Flow(origin='Mexico', destination='United States'),
    # Weekly U.S. Imports from Nigeria of Crude Oil
    'PET.W_EPC0_IM0_NUS-NNI_MBBLD.W': Flow(origin='Nigeria', destination='United States'),
    # Weekly U.S. Imports from Saudi Arabia of Crude Oil
    'PET.W_EPC0_IM0_NUS-NSA_MBBLD.W': Flow(origin='Saudi Arabia', destination='United States'),
    # Weekly U.S. Imports from Venezuela of Crude Oil
    'PET.W_EPC0_IM0_NUS-NVE_MBBLD.W': Flow(origin='Venezuela', destination='United States'),
}

# provider volume_unit: (kpler volume_unit, volume_map)
VOLUME_MAPPING = {
    # per day -> total in a week
    'Thousand Barrels per Day': ('barrel', lambda x: try_apply(x * 7 * 1000, int))
}


def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        series (Dict[str, Any]):

    Yields:
        Dict[str, str]:
    """
    item = map_keys(raw_item, flows_mapping())
    # source can provide zero volume, we still want to yield it
    if item['flow_volume'] is None:
        logger.warning(f'No data provided for series {item["id"]}: {item["weekly_date"]}')
        return

    # normalize import/export zones
    _id = item.pop('id')
    item.update(
        origin=SERIES_FLOW_MAPPING[_id].origin, destination=SERIES_FLOW_MAPPING[_id].destination
    )

    # create week date range from a single date
    item.update(
        zip(('flow_start', 'flow_end'), _obtain_week_range_from_date(item.pop('weekly_date')))
    )

    # normalize volume for the entire week
    item['flow_volume'], item['flow_volume_unit'] = _normalize_total_volume(
        item['flow_volume'], item['flow_volume_unit']
    )

    # TODO use a Flows model for validation and robustness
    return item


def flows_mapping():
    # declarative mapping for ease of development/maintenance
    return {
        'series_id': ('id', None),
        'name': ('product', lambda x: 'Crude Oil'),
        'units': ('flow_volume_unit', None),
        'f': ignore_key('irrelevant'),
        'unitsshort': ignore_key('shortname of volume unit; inaccurate'),
        'description': ignore_key('short description of series'),
        'copyright': ignore_key('irrelevant'),
        'source': ignore_key('provider'),
        'iso3166': ignore_key('3-letter country designation by ISO3166'),
        'geography': ignore_key('origin; not consistent'),
        'geography2': ignore_key('destination; not consistent'),
        'start': ignore_key('earliest datapoint available'),
        'end': ignore_key('latest datapoint available'),
        'updated': ('reported_date', lambda x: to_isoformat(x, dayfirst=False)),
        'x_value': ('weekly_date', lambda x: to_isoformat(x, dayfirst=False)),
        'y_value': ('flow_volume', None),
        'provider_name': ('provider_name', None),
    }


def _normalize_total_volume(raw_volume, raw_unit):
    """Normalize raw volume data based on total date range.

    TODO could use start/end date info to determine total volume, however no use case for it yet

    Args:
        raw_volume (int):
        raw_unit (str):

    Returns:
        Tuple[str, str]: tuple of (volume, volume_unit)

    Examples:
        >>> _normalize_total_volume(2640, 'Thousand Barrels per Day')
        (18480000, 'barrel')
        >>> _normalize_total_volume(2640, 'foobar')
        Traceback (most recent call last):
            ...
        ValueError: Unknown volume unit: foobar
    """
    unit, volume_map = VOLUME_MAPPING.get(raw_unit, (None, None))
    if not unit:
        raise ValueError(f'Unknown volume unit: {raw_unit}')

    return volume_map(raw_volume), unit


def _obtain_week_range_from_date(raw_date):
    """Obtain the start/end date of the week the specified date falls in.

    Start of week: Saturday 1200 hrs
    End of week:   Friday 1159 hrs

    NOTE Function is specific to EIA source, since metrics are reported in terms of weekly output.
         However, analysts want total volume across the entire week the metric is reported in.

    Args:
        raw_date (str):

    Returns:
        Tuple[str, str]: tuple of (start_date, end_date) of the week

    Examples:
        >>> _obtain_week_range_from_date('2018-09-21T00:00:00')
        ('2018-09-15T12:00:00', '2018-09-21T11:59:59')
        >>> _obtain_week_range_from_date('2018-09-14T00:00:00')
        ('2018-09-08T12:00:00', '2018-09-14T11:59:59')
    """
    end_date = parse_date(raw_date, dayfirst=False) + relativedelta(weekday=FR(-1))
    start_date = end_date + relativedelta(weekday=SA(-1))

    return (
        start_date.replace(hour=12, minute=00, second=00).isoformat(),
        end_date.replace(hour=11, minute=59, second=59).isoformat(),
    )
