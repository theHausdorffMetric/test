# -*- coding: utf-8 -*-

"""Airtable API wrapper for Kpler

Remark
~~~~~~

TODO Delete the spiders that are no longer exist

Note: Airtable API rate limits at 5 requests/s. Maybe we could use another wrapper.

The current wrapper we use https://github.com/nicocanali/airtable-python, does not manage rate
limiting hence we have to manage it ourselves.

https://github.com/gtalarico/airtable-python-wrapper
This wrapper, unlike the one we currently use, helps to do the rate limiting for us by having
a `batch_insert` method.

"""

from airtable import airtable

from kp_scrapers.cli.ui import info
from kp_scrapers.lib.services.shub import global_settings as Settings, validate_settings
from kp_scrapers.lib.utils import throttle


BASE_MAPPING = {'Data Sourcing': 'appY2FMxUDFExuYGK'}

# Airtable API rate limit is 5 requests per second, 0.2 is from empirical tests
RATE_LIMIT = 0.2


def connect(base):
    """API sugar for accessing table data by table name.

    For docs, see https://airtable.com/appY2FMxUDFExuYGK/api/docs

    Args:
        base (str):

    Returns:
        Airtable:

    """
    validate_settings('AIRTABLE_API_KEY')

    return airtable.Airtable(BASE_MAPPING[base], Settings()['AIRTABLE_API_KEY'])


def retrieve(base, table, **opts):
    """Retrieve single or multiple records from in Airtable table.

    Check out the response format here:
    https://airtable.com/appY2FMxUDFExuYGK/api/docs#curl/table:__test:list

    Args:
        base (str):
        table (str):
        **opts: define page size and offset here

    Returns:
        Tuple[str, str, str]: offset, fields, id

    """
    payload = connect(base).get(table, **opts)
    return ((payload.get('offset'), row['fields'], row['id']) for row in payload.get('records', []))


def retrieve_all_records(base, table, page_size=100):
    """Retrieve all the records in the table at one time, with id added.

    Args:
        base (str):
        table (str):
        page_size (int): 100 by default

    Yields:
        Dict[str, str]: only include non-empty fields

    """
    opts = {'limit': page_size}
    offset = None

    while True:
        for offset, fields, id in retrieve(base, table, **opts):
            fields.update(id=id)
            yield fields

        if offset:
            opts['offset'] = offset
        else:
            break


@throttle(seconds=RATE_LIMIT)
def create(base, table, row):
    """Create a record in Airtable and handle rate limit.

    Notes:
        If you get a HTTP Error, for example, 422 client error, you should make sure the column on
        Airtable:
            1) Exists
            2) Supports all options (multiple selects should be defined)
            3) Field type is correct (date and time both need to be specified in Airtable)

    Args:
        base (str):
        table (str):
        row (List[str]):

    Returns:

    """
    return connect(base).create(table, row)


def batch_create(base, table, data):
    """Create multiple records within rate limit.

    Args:
        base (str):
        table (str):
        data (List[List[str]]):

    Returns:
        None

    """
    for row in data:
        info(f'Creating row {row["Spider"]}')
        create(base, table, row)


@throttle(seconds=RATE_LIMIT)
def update(base, table, id, row):
    """Update an existing Airtable row with id given and handle rate limit.

    Notes:
        Debug HTTP Error same as `create` method.

    Args:
        base (str):
        table (str):
        id (str):
        row (List[str]):

    Returns:


    """
    return connect(base).update(table, id, row)


def batch_update(base, table, data):
    """Update multiple rows.

    Args:
        base (str):
        table (str):
        data (List[List[str]]):

    Returns:

    """
    for row, id in data:
        info(f'Updating row {row["Spider"]}')
        update(base, table, id, row)
