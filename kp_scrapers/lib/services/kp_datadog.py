"""Datadog API wrapper for Kpler to get metrics.

Remark
~~~~~~

Check out the list of possible metrics available [here](https://app.datadoghq.com/metric/summary).

"""

from collections import OrderedDict
from datetime import datetime
import time

from datadog import api, initialize

from kp_scrapers.lib.services.shub import global_settings as Settings, validate_settings


ONE_DAY = 86400


def query_metric(metric, start, end, group_by='host'):
    """Query the metric of a specific time duration.

    Notes:
        [Datadog documentations](https://docs.datadoghq.com/api/?lang=python#metrics).

    Args:
        metric (str):
        start (int):
        end (int):
        group_by (str):

    Returns:
        (query):

    """
    query = f'avg:{metric}{{*}}by{{{group_by}}}'
    return api.Metric.query(start=start, end=end, query=query)


def initialize_api():
    validate_settings('DATADOG_APP_KEY', 'DATADOG_API_KEY')

    initialize(api_key=Settings()['DATADOG_API_KEY'], app_key=Settings()['DATADOG_APP_KEY'])


def get_metric_by_spider(metric, window=ONE_DAY, group_by='spider_name'):
    """Query datadog metric for spiders.

    Notes:
        only tested with:
            1) kp.sh.spiders.stats.item_scraped_count
            2) kp.sh.spiders.stats.log_count.error

    Args:
        metric (str):
        window (int): default is one day
        group_by (str): default is spider name

    Returns:
        Dict[str, Dict[str, int]]

    """
    # extract metric from Datadog from 24 hours ago to now
    end = int(time.time())
    start = end - window
    raw_response = query_metric(metric, start, end, group_by=group_by)

    result = {}
    for spider_info in raw_response['series']:
        spider_name = spider_info['scope'].split(':')[1]

        result.update({spider_name: map_list_to_dict(spider_info['pointlist'])})

    return result


def map_list_to_dict(raw_list):
    """Map the raw list to dict with list items combined.

    The

    Notes:
        more info about the raw list format, check out here:
        https://docs.datadoghq.com/api/?lang=python#query-timeseries-points

    Args:
        raw_list (List[List[float]]): [[timestamp, value], ...]

    Returns:
        OrderedDict[str, int]: {isotime: value, ...}

    """
    return OrderedDict(
        {normalize_timestamp(x[0]): round(x[1]) for x in raw_list if x[1] is not None}
    )


def normalize_timestamp(timestamp):
    """Convert POSIX timestamp to ISO 8601 format.

    Args:
        timestamp (float):

    Returns:
        str: time in iso format

    """
    return datetime.fromtimestamp(timestamp / 1000).isoformat()
