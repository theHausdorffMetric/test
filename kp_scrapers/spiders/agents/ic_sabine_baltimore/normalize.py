import logging

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


MOVEMENT_MAPPING = {'Load': 'load', 'Discharge': 'discharge'}


INSTALLATION_MAPPING = {
    'cheniere': 'Sabine Pass',
    'enterprise east': 'Oiltank. Beaumont',
    'enterprise west': 'TE Products',
    'exxon': 'ExxonMobil Beaumont Refinery',
    'huntsman': 'Huntsman Port Arthur',
    'jefferson energy': 'Jefferson Energy Beaumont',
    'martin gas': 'Beaumont Industrial Park',
    'motiva': 'Oiltank. Neches',
    'orange dock': 'Orange Dock',
    'phillips 66': 'P66 Beaumont',
    'port of port arthur': 'Port of Port Arthur',
    'sla': 'Galveston Light.',
    'sunoco': 'Sunoco Nederland',
    'total': 'Total Port Arthur Refinery',
    'valero': 'Valero Port Arthur Refinery',
    'valero pleasure island': 'Valero Port Arthur Refinery',
    'lng (cove point)': 'Cove Point',
}


@validate_item(PortCall, normalize=True, strict=True, log_level='error')
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    if not item['vessel']['name']:
        return

    # since this spider processes 2 types of emails and zones are different for
    # lng and other platforms
    if 'sabine' in item['attachment_name'].lower():
        if item['installation'] == 'Sabine Pass':
            item['port_name'] = 'Sabine Pass'
        else:
            item['port_name'] = 'Beaumont/Port Arthur'

    if 'baltimore' in item['attachment_name'].lower():
        if item['installation'] == 'Cove Point':
            item['port_name'] = 'Cove Point'
        else:
            item['port_name'] = 'Baltimore'

    if 'new york' in item['attachment_name'].lower():
        item['port_name'] = 'New York'

    if 'seattle' in item['attachment_name'].lower():
        item['port_name'] = 'Seattle'

    if 'lake charles' in item['attachment_name'].lower():
        if item['installation'] == 'Cameron LNG (Lake Charles)':
            item['port_name'] = 'Cameron'
        else:
            item['port_name'] = 'Lake Charles'

    for col in ['eta', 'departure']:
        if item.get(col):
            item[col] = normalize_date(item[col], item['reported_date'])

    # if vessel in port, take reported date
    if 'in port' in item['status'].lower():
        item['berthed'] = item['reported_date']

    product = item.pop('cargo_product', None)
    movement = item.pop('cargo_movement', None)

    if product and not any(sub in product.lower() for sub in ('nil', 'tbd')):
        item['cargoes'] = [{'product': product, 'movement': movement}]

    for cleanup_col in ['attachment_name', 'status']:
        if item.get(cleanup_col):
            item.pop(cleanup_col, None)

    return item


def field_mapping():
    return {
        'Vessel': ('vessel', lambda x: {'name': None if 'NIL' in x else x}),
        'ETA': ('eta', None),
        'ETS': ('departure', None),
        'Status': ('status', None),
        'Operation': ('cargo_movement', lambda x: MOVEMENT_MAPPING.get(x, None)),
        'Origin/Destination': ignore_key('irrelevant'),
        'Commodity': ('cargo_product', None),
        'installation': ('installation', lambda x: INSTALLATION_MAPPING.get(x.lower(), x)),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'attachment_name': ('attachment_name', None),
    }


def normalize_date(raw_date, rpt_date):
    """eta dates do not contain the year

    Format:
    1. dd-b

    Args:
        raw_date (str):
        raw_reported_date (str):

    Examples:
        >>> normalize_date('3-Mar', '2019-04-03T10:01:00')
        '2019-03-03T00:00:00'
        >>> normalize_date('1-Jan', '2019-12-30T10:01:00')
        '2020-01-01T00:00:00'
        >>> normalize_date('30-Dec', '2020-01-01T10:01:00')
        '2019-12-30T00:00:00'

    Returns:
        str:
    """
    if len(raw_date.split('-')) == 2:

        _day, _month = raw_date.split('-')
        _year = parse_date(rpt_date).year

        _date = parse_date(f'{_day} {_month} {_year}', dayfirst=True)

        # to accomodate end of year parsing, prevent dates too old or far into
        # the future. 100 days was chosen as a gauge
        if (_date - parse_date(rpt_date)).days < -100:
            return parse_date(f'{_day} {_month} {_year + 1}', dayfirst=True).isoformat()

        if (_date - parse_date(rpt_date)).days > 100:
            return parse_date(f'{_day} {_month} {_year - 1}', dayfirst=True).isoformat()

        return _date.isoformat()

    return None
