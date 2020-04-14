from collections import namedtuple
import datetime as dt
import logging

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import FR, relativedelta, SA

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.market_figure import MarketFigure
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

DataSeries = namedtuple('DataSeries', ('country', 'balance', 'product', 'country_type'))

MARKET_FIGURE_MAPPING = {
    # Exports - Monthly
    'PET.MCREXP12.M': DataSeries(
        country='PADD 1', balance='Export', product='Crude Oil', country_type='region'
    ),
    'PET.MCREXP22.M': DataSeries(
        country='PADD 2', balance='Export', product='Crude Oil', country_type='region'
    ),
    'PET.MCREXP32.M': DataSeries(
        country='PADD 3', balance='Export', product='Crude Oil', country_type='region'
    ),
    'PET.MCREXP42.M': DataSeries(
        country='PADD 4', balance='Export', product='Crude Oil', country_type='region'
    ),
    'PET.MCREXP52.M': DataSeries(
        country='PADD 5', balance='Export', product='Crude Oil', country_type='region'
    ),
    # Imports
    'PET.MCRIMP12.M': DataSeries(
        country='PADD 1', balance='Import', product='Crude Oil', country_type='region'
    ),
    'PET.MCRIMP22.M': DataSeries(
        country='PADD 2', balance='Import', product='Crude Oil', country_type='region'
    ),
    'PET.MCRIMP32.M': DataSeries(
        country='PADD 3', balance='Import', product='Crude Oil', country_type='region'
    ),
    'PET.MCRIMP42.M': DataSeries(
        country='PADD 4', balance='Import', product='Crude Oil', country_type='region'
    ),
    'PET.MCRIMP52.M': DataSeries(
        country='PADD 5', balance='Import', product='Crude Oil', country_type='region'
    ),
    # Stock Change
    'PET.MCRSCP12.M': DataSeries(
        country='PADD 1', balance='Stock change', product='Crude Oil', country_type='region'
    ),
    'PET.MCRSCP22.M': DataSeries(
        country='PADD 2', balance='Stock change', product='Crude Oil', country_type='region'
    ),
    'PET.MCRSCP32.M': DataSeries(
        country='PADD 3', balance='Stock change', product='Crude Oil', country_type='region'
    ),
    'PET.MCRSCP42.M': DataSeries(
        country='PADD 4', balance='Stock change', product='Crude Oil', country_type='region'
    ),
    'PET.MCRSCP52.M': DataSeries(
        country='PADD 5', balance='Stock change', product='Crude Oil', country_type='region'
    ),
    # Refinery Net Input
    'PET.M_EPC0_YIY_R10_2.M': DataSeries(
        country='PADD 1', balance='Refinery intake', product='Crude Oil', country_type='region'
    ),
    'PET.M_EPC0_YIY_R20_2.M': DataSeries(
        country='PADD 2', balance='Refinery intake', product='Crude Oil', country_type='region'
    ),
    'PET.M_EPC0_YIY_R30_2.M': DataSeries(
        country='PADD 3', balance='Refinery intake', product='Crude Oil', country_type='region'
    ),
    'PET.M_EPC0_YIY_R40_2.M': DataSeries(
        country='PADD 4', balance='Refinery intake', product='Crude Oil', country_type='region'
    ),
    'PET.M_EPC0_YIY_R50_2.M': DataSeries(
        country='PADD 5', balance='Refinery intake', product='Crude Oil', country_type='region'
    ),
    # Production
    'PET.MCRFPP12.M': DataSeries(
        country='PADD 1', balance='Production', product='Crude Oil', country_type='region'
    ),
    'PET.MCRFPP22.M': DataSeries(
        country='PADD 2', balance='Production', product='Crude Oil', country_type='region'
    ),
    'PET.MCRFPP32.M': DataSeries(
        country='PADD 3', balance='Production', product='Crude Oil', country_type='region'
    ),
    'PET.MCRFPP42.M': DataSeries(
        country='PADD 4', balance='Production', product='Crude Oil', country_type='region'
    ),
    'PET.MCRFP5F2.M': DataSeries(
        country='PADD 5', balance='Production', product='Crude Oil', country_type='region'
    ),
    # Refinery Net Input (WEEKLY)
    'PET.WCRRIP12.W': DataSeries(
        country='PADD 1', balance='Refinery intake', product='Crude Oil', country_type='region'
    ),
    'PET.WCRRIP22.W': DataSeries(
        country='PADD 2', balance='Refinery intake', product='Crude Oil', country_type='region'
    ),
    'PET.WCRRIP32.W': DataSeries(
        country='PADD 3', balance='Refinery intake', product='Crude Oil', country_type='region'
    ),
    'PET.WCRRIP42.W': DataSeries(
        country='PADD 4', balance='Refinery intake', product='Crude Oil', country_type='region'
    ),
    'PET.WCRRIP52.W': DataSeries(
        country='PADD 5', balance='Refinery intake', product='Crude Oil', country_type='region'
    ),
    # Commercial Crude Oil Imports - Weekly
    'PET.WCEIMP12.W': DataSeries(
        country='PADD 1', balance='Import', product='Crude Oil', country_type='region'
    ),
    'PET.WCEIMP22.W': DataSeries(
        country='PADD 2', balance='Import', product='Crude Oil', country_type='region'
    ),
    'PET.WCEIMP32.W': DataSeries(
        country='PADD 3', balance='Import', product='Crude Oil', country_type='region'
    ),
    'PET.WCEIMP42.W': DataSeries(
        country='PADD 4', balance='Import', product='Crude Oil', country_type='region'
    ),
    'PET.WCEIMP52.W': DataSeries(
        country='PADD 5', balance='Import', product='Crude Oil', country_type='region'
    ),
    # Ending Stocks excluding SPR - Weekly
    'PET.WCESTP11.W': DataSeries(
        country='PADD 1', balance='Ending stocks', product='Crude Oil', country_type='region'
    ),
    'PET.WCESTP21.W': DataSeries(
        country='PADD 2', balance='Ending stocks', product='Crude Oil', country_type='region'
    ),
    'PET.WCESTP31.W': DataSeries(
        country='PADD 3', balance='Ending stocks', product='Crude Oil', country_type='region'
    ),
    'PET.WCESTP41.W': DataSeries(
        country='PADD 4', balance='Ending stocks', product='Crude Oil', country_type='region'
    ),
    'PET.WCESTP51.W': DataSeries(
        country='PADD 5', balance='Ending stocks', product='Crude Oil', country_type='region'
    ),
    # Ending Stocks - weekly
    'PET.WCSSTUS1.W': DataSeries(
        country='US SPR', balance='Ending stocks', product='Crude Oil', country_type='storage'
    ),
}

CATEGORY_MAPPING = {
    '235898': (
        'PET.WCESTP11.W',
        'PET.WCESTP21.W',
        'PET.WCESTP31.W',
        'PET.WCESTP41.W',
        'PET.WCESTP51.W',
    ),
    '236136': (
        'PET.WCEIMP12.W',
        'PET.WCEIMP22.W',
        'PET.WCEIMP32.W',
        'PET.WCEIMP42.W',
        'PET.WCEIMP52.W',
    ),
    '235683': (
        'PET.WCRRIP12.W',
        'PET.WCRRIP22.W',
        'PET.WCRRIP32.W',
        'PET.WCRRIP42.W',
        'PET.WCRRIP52.W',
    ),
    '296686': (
        'PET.MCRFPP12.M',
        'PET.MCRFP5F2.M',
        'PET.MCRFPP32.M',
        'PET.MCRFPP22.M',
        'PET.MCRFPP42.M',
    ),
    '298623': (
        'PET.M_EPC0_YIY_R30_2.M',
        'PET.M_EPC0_YIY_R40_2.M',
        'PET.M_EPC0_YIY_R50_2.M',
        'PET.M_EPC0_YIY_R20_2.M',
    ),
    '298436': ('PET.M_EPC0_YIY_R10_2.M'),
}


@validate_item(MarketFigure, normalize=True, strict=False, log_level='debug')
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        series (Dict[str, Any]):

    Yields:
        Dict[str, str]:
    """
    item = map_keys(raw_item, eia_mapping())

    # source can provide zero volume, we still want to yield it
    if item['volume'] is None:
        logger.warning(f'No data provided for series {item["id"]}: {item["weekly_date"]}')
        return

    # normalize import/export zones
    _id = item.pop('id')
    item.update(
        country=MARKET_FIGURE_MAPPING[_id].country,
        product=MARKET_FIGURE_MAPPING[_id].product,
        balance=MARKET_FIGURE_MAPPING[_id].balance,
    )

    if MARKET_FIGURE_MAPPING[_id].country_type:
        item.update(country_type=MARKET_FIGURE_MAPPING[_id].country_type)

    raw_date = item.pop('weekly_date')

    if len(raw_date) == 6:  # only the month range is given
        year = int(raw_date[0:4])
        month = int(raw_date[4:6])
        item['start_date'], item['end_date'], numdays = _get_period(f'{year}-{month}')
    else:
        date_formatted = to_isoformat(raw_date, dayfirst=False)
        # create week date range from a single date
        item.update(zip(('start_date', 'end_date'), _obtain_week_range_from_date(date_formatted)))
        numdays = 7

    # normalize volume for the entire week
    item['volume'], item['unit'] = _normalize_total_volume(item['volume'], item['unit'], numdays)

    yield item


def eia_mapping():
    # declarative mapping for ease of development/maintenance
    return {
        'series_id': ('id', None),
        'name': ('product', None),
        'units': ('unit', None),
        'f': ignore_key('irrelevant'),
        'unitsshort': ignore_key('shortname of volume unit; inaccurate'),
        'description': ignore_key('short description of series'),
        'copyright': ignore_key('irrelevant'),
        'source': ignore_key('provider'),
        'iso3166': ignore_key('3-letter country designation by ISO3166'),
        'geography': ignore_key('country; not consistent'),
        'geography2': ignore_key('; not consistent'),
        'start': ignore_key('earliest datapoint available'),
        'end': ignore_key('latest datapoint available'),
        'updated': ('reported_date', lambda x: to_isoformat(x, dayfirst=False)),
        'x_value': ('weekly_date', None),
        'y_value': ('volume', None),
        'provider_name': ('provider_name', None),
    }


def _normalize_total_volume(raw_volume, raw_unit, numdays):
    """Normalize raw volume data based on total date range.

    TODO could use start/end date info to determine total volume, however no use case for it yet

    Args:
        raw_volume (int):
        raw_unit (str):

    Returns:
        Tuple[str, str]: tuple of (volume, volume_unit)

    Examples:
        >>> _normalize_total_volume(2640, 'Thousand Barrels per Day', 7)
        (18480000, 'barrel')
        >>> _normalize_total_volume(2640, 'Thousand Barrels per Day', 30)
        (79200000, 'barrel')
        >>> _normalize_total_volume(2640, 'foobar', 7)
        Traceback (most recent call last):
            ...
        ValueError: Unknown volume unit: foobar
    """
    if raw_unit == 'Thousand Barrels per Day':
        volume = try_apply(raw_volume * numdays * 1000, int)
    elif raw_unit == 'Thousand Barrels':
        volume = try_apply(raw_volume * 1000, int)
    else:
        raise ValueError(f'Unknown volume unit: {raw_unit}')

    return volume, 'barrel'


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
        ('2018-09-15T00:00:00', '2018-09-21T00:00:00')
        >>> _obtain_week_range_from_date('2018-09-14T00:00:00')
        ('2018-09-08T00:00:00', '2018-09-14T00:00:00')
    """
    end_date = parse_date(raw_date, dayfirst=False) + relativedelta(weekday=FR(-1))
    start_date = end_date + relativedelta(weekday=SA(-1))

    return (start_date.isoformat(), end_date.isoformat())


def _get_period(raw_period):
    """Get a DateTimeRange given a raw period string.
    Examples:  # noqa
        >>> _get_period('2019-06')
        ('2019-06-01T00:00:00', '2019-07-01T00:00:00', 30)
        >>> _get_period('2019-12')
        ('2019-12-01T00:00:00', '2020-01-01T00:00:00', 31)
    """
    # assumption is that periods are formatted as `YYYY-MM`
    year, _, month = raw_period.partition('-')

    # get lower bound
    lower_bound = dt.datetime(year=int(year), month=int(month), day=1)

    # get upper bound (also account for year rollover if month is december)
    year = str(int(year) + 1) if month == '12' else year
    month = '1' if month == '12' else str(int(month) + 1)
    upper_bound = dt.datetime(year=int(year), month=int(month), day=1)

    # default: lower_bound is inclusive, upper_bound is exclusive
    return lower_bound.isoformat(), upper_bound.isoformat(), (upper_bound - lower_bound).days
