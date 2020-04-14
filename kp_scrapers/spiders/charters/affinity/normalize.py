from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import is_isoformat, to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.utils import validate_item


@validate_item(SpotCharter, normalize=True, strict=True, log_level='error')
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())

    # remove vessels with no imo
    if not item['vessel']['imo']:
        return

    # remove `cargo` field if we have None `product`
    if not item['cargo']['product']:
        item.pop('cargo')

    return item


def field_mapping():
    return {
        'Cargo Type': ('cargo', lambda x: {'product': may_strip(x)}),
        'Cargo Type Name': ('cargo', lambda x: {'product': may_strip(x)}),
        'Cgo': ('cargo', lambda x: {'product': may_strip(x)}),
        'Charterer': ('charterer', may_strip),
        'Date': ('reported_date', lambda x: parse_date(x).strftime('%d %b %Y')),
        'From': ('departure_zone', may_strip),
        'From Port': ('departure_zone', may_strip),
        'From Zn': (ignore_key('redundant departure zone')),
        'From Zone Code': (ignore_key('redundant departure zone')),
        'IMO': ('vessel', lambda x: {'imo': x}),
        'IMO ': ('vessel', lambda x: {'imo': x}),
        'IMO #': ('vessel', lambda x: {'imo': x}),
        'IMONumber': ('vessel', lambda x: {'imo': x}),
        'Lay Can From': ('lay_can_start', lambda x: normalize_lay_can(x)),
        'Lay Can To': ('lay_can_end', lambda x: normalize_lay_can(x)),
        'LayCan From': ('lay_can_start', lambda x: normalize_lay_can(x)),
        'LayCan To': ('lay_can_end', lambda x: normalize_lay_can(x)),
        'provider_name': ('provider_name', may_strip),
        'Reported Date': ('reported_date', lambda x: parse_date(x).strftime('%d %b %Y')),
        'To': ('arrival_zone', lambda x: [may_strip(zone) for zone in x.split('-')] if x else []),
        'To Port': (
            'arrival_zone',
            lambda x: [may_strip(zone) for zone in x.split('-')] if x else [],
        ),
        'To Zn': (ignore_key('redundant arrival zone')),
        'To Zone Code': (ignore_key('redundant arrival zone')),
    }


def normalize_lay_can(date_item):
    """Transform non isoformat dates to isoformat

    Examples:
        >>> normalize_lay_can('04-Feb-2018')
        '2018-02-04T00:00:00'
        >>> normalize_lay_can('2018-02-04T00:00:00')
        '2018-02-04T00:00:00'

    Args:
        date_item (str):

    Returns:
        str:
    """
    return date_item if is_isoformat(date_item) else to_isoformat(date_item, dayfirst=True)
