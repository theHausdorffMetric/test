import datetime as dt
import logging
import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

PRODUCT_BLACKLIST = [
    'cruceristas',
    'eolicos',
    'dragado de puerto',
    'general',
    'maquinaria',
    'papel',
    'para plataf',
    'productos',
    'provisiones',
    'teus',
]


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Yields:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    # discard vessels carrying irrelevant cargoes
    if not item.get('load') and not item.get('discharge'):
        logger.debug(f"Vessel is carrying irrelevant cargo: {item['vessel_name']}")
        return

    # build cargoes sub-model
    item['cargoes'] = item.pop('load') + item.pop('discharge')

    # build vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'length': item.pop('vessel_length'),
        'gross_tonnage': item.pop('vessel_gt'),
    }

    # build proper ETA
    item['eta'] = normalize_date(item['eta'], item['reported_date'])

    return item


def field_mapping():
    return {
        'C.N.': ignore_key('internal port call number used by source'),
        'Consignmen': ignore_key('consignee'),
        'Destination': ignore_key('next portcall; to discuss with analysts'),
        'Flag': ignore_key('vessel flag'),
        'Home port': ignore_key('vessel home port'),
        'LgthG.': ('vessel_length', None),
        'Loading': ('load', lambda x: list(normalize_cargo(x, 'load'))),
        'M.S.': ignore_key('unknown'),
        'Name': ('vessel_name', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        # each separate product has duplicated eta date from the source, we only need one
        'S.D.': ('eta', lambda x: x.split()[0]),
        'St tevedore': ignore_key('stevedore'),
        'T': ('vessel_gt', None),
        'Unloading': ('discharge', lambda x: list(normalize_cargo(x, 'discharge'))),
    }


def normalize_cargo(raw_cargo_str, movement):
    """Normalize raw cargo string into a proper Cargo object.

    Raw cargo strings come usually in this format:
        - <volume> <cargo> (one cargo onboard)
        - <volume> <cargo> <volume> <cargo> (two cargoes onboard)
        - etc.

    The <cargo> substring may not be present sometimes.
    In such cases, we don't process the cargo associated with that volume (i.e. not possible
    to even extract a cargo if <cargo> is missing).

    """
    cargoes = re.findall(r'(\d+ [a-zA-Z\s-]+)', raw_cargo_str.strip())
    for cargo in cargoes:
        volume, _, product = cargo.partition(' ')

        # filter and discard irrelevant cargo
        if product.strip() in PRODUCT_BLACKLIST:
            return

        # filter and discard zero volumes
        if volume.strip() == '0':
            return

        yield {
            'product': product.strip(),
            'movement': movement,
            'volume': volume.strip(),
            'volume_unit': Unit.tons,
        }


def normalize_date(raw_date, reported_date):
    """Normalize matching date given date and month info.

    See `tests.spiders.port_authorities.test_bilbao` for test cases.

    A month is given in the table rows, so it is slightly easier to get the actual
    matching_date. However, the rollover problem exists here as well.
    No year is given, so it needs to be inferred indirectly.

    Some examples of possible input combinations (format of DD-MM):
        - 'eta': '01-03'
        - 'eta': '4-11'
        - 'eta': '12-12'

    To solve this, we compare if date is in the past.
        - Reject if computed date is in the past.
          Then, return computed date adjusted 1 year in the future.

    `reported_date` is required here as a basis of comparison for past/future dates.

    Args:
        raw_date (str): raw portcall date
        reported_date (str): reported date formatted in ISO-8601

    Returns:
        str: matching date in ISO-8601 format

    """
    # memoise reported_date
    reported_date = parse_date(reported_date, dayfirst=False)
    day, month = raw_date.split('-')
    matching_date = dt.datetime.combine(
        reported_date.replace(day=int(day), month=int(month)), dt.time.min
    )

    # matching date is in the past, let's set the date range loose
    # as matching_date could be earlier than reported date
    if matching_date - reported_date < dt.timedelta(days=-7):
        matching_date += relativedelta(years=1)

    return matching_date.isoformat()
