import datetime as dt
import logging
import re

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.tropical_storm import TropicalStorm
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(TropicalStorm, normalize=True, strict=True)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]: normalized cargo movement item

    """
    item = map_keys(raw_item, field_mapping())
    item['report_utc'] = item['raw_report_date']
    item['expiration_utc'] = (
        item['raw_last_forecast_date']
        if item['raw_last_forecast_date']
        else item['report_utc'] + dt.timedelta(days=3)
    )
    item['forecast'] = __parse_polygon(item['raw_forecast'])
    item['longitude'], item['latitude'] = normalize_point_coordinates(item.get('raw_position'))
    item['forecast_data'] = normalize_forecast_data(item.get('forecast_data'))

    return item


def field_mapping():
    # declarative mapping for ease of developement/maintenance
    return {
        'name': ('name', lambda x: may_strip(x)),
        'pretty_name': ('pretty_name', None),
        'description': ('description', None),
        'forecast_data': ('forecast_data', None),
        'raw_report_date': ('raw_report_date', None),
        'raw_last_forecast_date': ('raw_last_forecast_date', None),
        'raw_position': ('raw_position', None),
        'raw_forecast': ('raw_forecast', None),
        'winds_sustained': (
            'winds_sustained',
            lambda x: re.match(r'(\d+)', x).group(1) if x else None,
        ),
        'winds_gust': ('winds_gust', lambda x: re.match(r'(\d+)', x).group(1) if x else None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def __parse_polygon(raw_forecast_string):
    """raw forecast is in the form of 58.2300,-19.0200,0 58.2900,0
    this function serves to extract and format the coordinates into a
    polygon string

    Args:
        raw_forecast_string (str):

    Returns
        str:
    """
    if not raw_forecast_string:
        return None

    find_all_coord = re.findall(r'(-?\d+.\d+),(-?\d+.\d+),0[\s]*', may_strip(raw_forecast_string))
    poly = ','.join(' '.join(coord) for coord in find_all_coord)

    return f'POLYGON(({poly}))'


def normalize_point_coordinates(raw_coord_position):
    """normalize string coordinates

    Args:
        raw_coord_position (str):

    Returns
        Tuple[str, str]:

    Examples:
        >>> normalize_point_coordinates('058.0,-21.1,0')
        (58.0, -21.1)
    """
    _long, _lat, _ = re.split(r'[, ]+', raw_coord_position)
    return float(_long), float(_lat)


def normalize_forecast_data(forecast_list_dict):
    """normalize list of dictionaries

    Args:
        raw_coord_position (List[Dict[str, str]]):

    Returns
        List[Dict[str, str]]:

    Examples:
        >>> normalize_forecast_data([{'date': '2020-01-02T00:00:00', 'wind': '50 KTS', 'position': '-33, 63.3, 0'}]) # noqa
        [{'date': '2020-01-02T00:00:00', 'position': {'lat': 63.3, 'lon': -33.0}, 'wind': 50.0}]
    """
    forecast_data = []
    for elt in forecast_list_dict or []:
        lon, lat, _ = tuple(float(e) for e in elt['position'].split(','))
        wind = float(re.match(r'(\d+)', elt['wind']).group(1)) if elt.get('wind') else None

        forecast_data.append(
            {'date': elt['date'], 'position': {'lat': lat, 'lon': lon}, 'wind': wind}
        )

    return forecast_data if forecast_data else None
