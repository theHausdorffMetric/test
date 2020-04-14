import datetime as dt
import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.parser import may_strip, try_apply
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
    item['report_utc'] = normalize_reported_date(item['raw_report_date'], item['reported_date'])
    item['expiration_utc'] = (
        normalize_expiration_date(item['raw_last_forecast_date'], item['reported_date'])
        if item['raw_last_forecast_date']
        else item['report_utc']
    )
    item['forecast'] = __parse_polygon(item['raw_forecast'])
    item['longitude'], item['latitude'] = normalize_point_coordinates(item.get('raw_position'))
    item['forecast_data'] = normalize_forecast_data(item.get('forecast_data'))

    return item


def field_mapping():
    # declarative mapping for ease of developement/maintenance
    return {
        'name': ('name', lambda x: normalize_name(x)),
        'pretty_name': ('pretty_name', None),
        'description': ('description', None),
        'forecast_data': ('forecast_data', None),
        'raw_report_date': ('raw_report_date', None),
        'raw_last_forecast_date': ('raw_last_forecast_date', None),
        'raw_position': ('raw_position', None),
        'raw_forecast': ('raw_forecast', None),
        'winds_sustained': ('winds_sustained', lambda x: try_apply(x, int) if x else 0),
        'winds_gust': ('winds_gust', lambda x: try_apply(x, int) if x else 0),
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


def normalize_name(raw_name):
    """normalize cyclone raw code names
    # "".join(re.search(r"(\w+)(?:[- ].*[- ])?(\w+)", item.get("name")).groups())
    Args:
        raw_name (str):

    Returns
        str:

    Examples:
        >>> normalize_name('202005S')
        '202005S'
    """
    return ''.join(re.search(r"(\w+)(?:[- ].*[- ])?(\w+)", raw_name).groups())


def normalize_reported_date(raw_rpt_date, raw_current_date):
    """normalize reported dates
    - 310000Z

    Args:
        raw_rpt_date (str):

    Returns
        str:

    """
    curr_date = parse_date(raw_current_date)
    return dt.datetime(
        year=curr_date.year,
        month=curr_date.month,
        day=int(raw_rpt_date[:2] or dt.datetime.now().day),
        hour=int(raw_rpt_date[2:4] or 12),
        minute=int(raw_rpt_date[4:6] or 0),
    ).isoformat()


def normalize_expiration_date(raw_exp_date, raw_current_date):
    """normalize expiration dates
    - 310000Z

    Args:
        raw_exp_date (str):

    Examples:
    >>> normalize_expiration_date('14/18Z', '2020-02-10T00:00:00')
    '2020-02-14T18:00:00'

    Returns
        str:

    """
    curr_date = parse_date(raw_current_date)
    exp_date = dt.datetime(
        year=curr_date.year,
        month=curr_date.month,
        day=int(raw_exp_date[:2] or dt.datetime.now().day),
        hour=int(raw_exp_date[3:5]),
    )

    if exp_date < curr_date:
        exp_date += relativedelta(months=1)

    return exp_date.isoformat()


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
        >>> normalize_forecast_data([{'date': 'TAU 12 - 2020010200Z', 'wind': '50 KTS', 'position': '-33 S, 063.3 E'}]) # noqa
        [{'date': '2020-01-02T00:00:00', 'position': {'lat': -33.0, 'lon': 63.3}, 'wind': 50.0}]
    """
    forecast_data = []
    for elt in forecast_list_dict or []:
        date = re.search(r'- (\d{4})(\d{2})(\d{2})(\d{2})', elt.get('date'))
        date = dt.datetime(
            int(date.group(1)), int(date.group(2)), int(date.group(3)), int(date.group(4))
        )
        pos = re.search(r'(-?\d+.?\d*) ., (-?\d+.?\d*) .', elt.get('position'))
        (lat, lon) = (float(pos.group(1)), float(pos.group(2)))
        wind = elt.get('wind').split(' ')[0]
        forecast_data.append(
            {
                'date': date.strftime('%Y-%m-%dT%H:00:00'),
                'position': {'lat': lat, 'lon': lon},
                'wind': float(wind),
            }
        )

    return forecast_data if forecast_data else None
