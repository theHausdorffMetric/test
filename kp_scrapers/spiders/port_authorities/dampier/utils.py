import datetime as dt
import json
import random

from kp_scrapers.settings.network import USER_AGENT_LIST


def headers():
    # try and mock as much as possible a real request, since Dampier has CDN protection
    return {
        'Accept': (
            'application/json,text/javascript,'
            'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        ),
        'Accept-Encoding': 'gzip,deflate,br',
        'Accept-Language': 'en-GB,en;q=0.5',
        'Cache-Control': 'max-age=0',
        'Content-Type': 'application/json;charset=utf-8',
        'Connection': 'keep-alive',
        'DNT': '1',
        'Host': 'kleinpod.pilbaraports.com.au',
        'TE': 'Trailers',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': random.choice(USER_AGENT_LIST),
    }


def request_body(code):
    """Build request body for querying Dampier's port schedule.

    Args:
        code (str | int):

    Returns:
        str: serialised json request body

    """
    body = {'reportCode': f'AUDAM-WEB-000{code}'}
    # different report types require different request params
    if str(code) == '2':
        body.update(
            parameters=[
                {'sName': 'START_DATE', 'aoValues': [{'Value': _get_dampier_time()}]},
                {'sName': 'END_DATE', 'aoValues': [{'Value': _get_dampier_time(days=7)}]},
            ]
        )

    return json.dumps(body)


def _get_dampier_time(**offset):
    """Get local time at Dampier port, with optional offset.

    Args:
        offset: keyword arguments for `dt.timedelta`

    Returns:
        str: ISO-8601 formatted datetime string

    """
    return (
        dt.datetime.utcnow().replace(second=0, microsecond=0) + dt.timedelta(**offset)
    ).isoformat()
