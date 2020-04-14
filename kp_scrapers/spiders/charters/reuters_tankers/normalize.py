from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


PRODUCT_IRRELEVANT_SUBSTRING = ['offshore', 'tanker', 'terminal']


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw workbook row into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
       Dict[str, str]: Kpler standardized SpotCharter item

    """
    item = map_keys(raw_item, spot_charter_mapping(), skip_missing=True)

    # build vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'imo': item.pop('imo')}

    # build cargo sub-model
    item['cargo'] = {
        'product': item.pop('product', None),
        'movement': 'load',
        'volume': item.pop('volume', None),
        'volume_unit': Unit.tons,
    }

    # if spider name is RS_WAF, assume that product is crude oil if it is empty
    # business assumption
    if item['spider'] == 'RS_WAF' and not item['cargo']['product']:
        item['cargo']['product'] = 'Crude Oil'

    # more comprehensive `rate_raw_value`
    item['rate_raw_value'] = '{} {}'.format(item['rate_raw_value'], item['rate_value'])

    # source sometimes provides precise port and other times vague geographical zones
    # at least one will be present at all times
    # take zone if port data not provided
    if not item['departure_zone']:
        item['departure_zone'] = item['departure_zone_broad']
    if not item['arrival_zone']:
        item['arrival_zone'] = item['arrival_zone_broad']

    # as required by SpotCharter model
    item['arrival_zone'] = item['arrival_zone'].split('-') if item['arrival_zone'] else None

    # clean up
    for x in ['departure_zone_broad', 'arrival_zone_broad', 'spider']:
        item.pop(x)

    return item


def spot_charter_mapping():
    return {
        'charterer': ('charterer', may_strip),
        'vessel': ('vessel_name', may_strip),
        'imo number': ('imo', lambda x: try_apply(x, float, int)),
        'cargo size': ('volume', None),
        'clean / dirty': (ignore_key('product type')),
        'commodity': ('product', lambda x: normalize_product(x) if x else None),
        'load zone': ('departure_zone_broad', may_strip),
        'load port': ('departure_zone', may_strip),
        'discharge zone': ('arrival_zone_broad', may_strip),
        'discharge port': ('arrival_zone', may_strip),
        'rate type': ('rate_raw_value', may_strip),
        'rate': ('rate_value', may_strip),
        'laycan': (
            'lay_can_start',
            lambda x: to_isoformat(x, dayfirst=False, yearfirst=True) if x else None,
        ),
        'reported_date': ('reported_date', None),
        'provider_name': ('provider_name', None),
        'spider': ('spider', None),
    }


def normalize_product(raw_product_name):
    """Remove irrelevant words from product name

    Args:
        str (raw_name):

    Returns:
        str: formatted product name

    Examples:
        >>> normalize_product('Escravos Oil Terminal')
        'escravos oil'
        >>> normalize_product('Agbami offshore tanker terminal')
        'agbami'

    """
    formatted_word = []
    for word in raw_product_name.lower().split(' '):
        if word in PRODUCT_IRRELEVANT_SUBSTRING:
            word = word.replace(word, '')

        formatted_word.append(word)

    return may_strip(' '.join(formatted_word))
