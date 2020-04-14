from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter, SpotCharterStatus
from kp_scrapers.models.utils import validate_item


STATUS_MAPPING = {
    'SUBS': SpotCharterStatus.on_subs,
    'ON SUBS': SpotCharterStatus.on_subs,
    'ON HOLD': SpotCharterStatus.on_subs,
    'FXD': SpotCharterStatus.fully_fixed,
}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]] | None

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    # irrelevant vessel
    if not item['vessel']:
        return

    # irrelevant info in departure_zone field
    if item.get('departure_zone') and 'SAILED' in item['departure_zone']:
        return

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can'), item['reported_date']
    )

    # deal with multi voyage
    if item.get('multi_voyage'):
        item['departure_zone'], item['arrival_zone'] = normalize_multi_voyage(
            item.pop('voyage'), item.pop('multi_voyage')
        )

    # single voyage
    if item.get('voyage'):
        item['departure_zone'], item['arrival_zone'] = normalize_voyage(item.pop('voyage'))

    return item


def field_mapping():
    return {
        'vessel': ('vessel', lambda x: None if 'TBN' in x.split() else {'name': x}),
        'charterer': ('charterer', normalize_charterer),
        'cargo': ('cargo', lambda x: {'product': x}),
        'voyage': ('voyage', None),
        'multi_voyage': ('multi_voyage', None),
        'departure_zone': ('departure_zone', None),
        'lay_can': ('lay_can', None),
        'rate_value': ('rate_value', None),
        'status': ('status', lambda x: STATUS_MAPPING[x]),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_charterer(raw_charterer):
    """Normalize charterer, remove the string after -.

    Args:
        raw_charterer (str):

    Returns:
        str:

    """
    charterer, _, _ = raw_charterer.partition('-')
    return charterer


def normalize_voyage(raw_voyage):
    """Normalize voyage, splitting departure_zone and arrival_zone.

    Examples:
        >>> normalize_voyage('SIKKA/JAPAN')
        ('SIKKA', ['JAPAN'])
        >>> normalize_voyage('VADINAR/AG-SUEZ')
        ('VADINAR', ['AG', 'SUEZ'])

    Args:
        raw_voyage (str):

    Returns:
        Tuple(str, List[str]):

    """
    departure_zone, _, arrival_zone = raw_voyage.partition('/')
    return departure_zone, arrival_zone.split('-')


def normalize_multi_voyage(*voyages):
    """Deal with multi voyage.

    Examples:
        >>> normalize_multi_voyage('AG/JAPAN', 'SIKKA/JAPAN')
        ('AG,SIKKA', ['JAPAN'])

    Args:
        *voyages: multi voyage, the format is <departure_zone>-<arrival_zone>

    Returns:

    """
    departure_zone = []
    arrival_zone = []

    for voyage in voyages:
        dpt, arr = normalize_voyage(voyage)

        if dpt not in departure_zone:
            departure_zone.append(dpt)

        for a in arr:
            if a not in arrival_zone:
                arrival_zone.append(a)

    return ','.join(departure_zone), arrival_zone


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reference of reported date.

    Examples:
        >>> normalize_lay_can('29-31 AUG', '27 August 2018')
        ('2018-08-29T00:00:00', '2018-08-31T00:00:00')
        >>> normalize_lay_can('08 SEP', '27 August 2018')
        ('2018-09-08T00:00:00', '2018-09-08T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple(str):

    """
    day, _, month = raw_lay_can.partition(' ')
    if not month:
        return None, None
    month = parse_date(month).month

    year = parse_date(reported).year
    if 'Dec' in raw_lay_can and 'Jan' in reported:
        year -= 1
    if 'Jan' in raw_lay_can and 'Dec' in reported:
        year += 1

    # month shift if it's a date range
    start_day, _, end_day = day.partition('-')
    start_day, end_day = try_apply(start_day, int), try_apply(end_day, int)

    start_month = month
    if start_day and end_day and start_day > end_day:
        start_month = 12 if int(month) - 1 == 0 else int(month) - 1

    lay_can_start = to_isoformat(f'{start_day} {start_month} {year}')
    lay_can_end = to_isoformat(f'{end_day} {month} {year}') if end_day else lay_can_start

    return lay_can_start, lay_can_end
