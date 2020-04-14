import datetime as dt

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


CARGO_BLACKLIST = ['G.CARGO', 'GENERAL CARGO', 'DETAINED', 'CONTRS.', 'NIL', 'WAITING']


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into relevant event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(
        raw_item, field_mapping(reported_date=raw_item['reported_date']), skip_missing=True
    )

    cargoes = list(normalize_cargoes(item))
    if not cargoes:
        return

    port_call = {
        'vessel': {'name': item['vessel_name']},
        'cargoes': cargoes,
        'reported_date': item['reported_date'],
        'port_name': item['port_name'],
        'provider_name': item['provider_name'],
        'arrival': item['arrival'],
    }

    if item.get('berthed'):
        port_call.update(
            berthed=item['berthed'], arrival=get_actual_arrival(item['berthed'], item['arrival'])
        )

    if item.get('departure'):
        port_call.update(departure=item['departure'])

    return port_call


def field_mapping(**kwargs):
    return {
        'Vessel Name': ('vessel_name', None),
        'Flag': ignore_key('vessel flag'),
        'Vessel  Details Disch.Agent/ Load.  Agent': ignore_key('shipping agent'),
        'Arrival Date': ('arrival', lambda x: normalize_date(x, **kwargs)),
        'Sailing Date': ('departure', lambda x: normalize_date(x, **kwargs)),
        'Comm. D/L': ('berthed', lambda x: normalize_date(x, **kwargs)),
        'Operations Berth No.': ignore_key('berth of portcall'),
        'M/T TONS': ('ton', None),
        'Cargo Disch .Details Commodity': ('commodity_d', None),
        'Cargo Load.Details Commodity': ('commodity_l', None),
        'reported_date': ('reported_date', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
    }


def normalize_date(date_str, reported_date):
    """Normalize date information to ISO 8601 format.

    Examples:
        >>> normalize_date('15.7.', '2018-07-25T00:00:00')
        '2018-07-15T00:00:00'
        >>> normalize_date('25.7.', '2018-07-25T00:00:00')
        '2018-07-25T00:00:00'
        >>> normalize_date('2.12.', '2018-07-25T00:00:00')
        '2017-12-02T00:00:00'
        >>> normalize_date('26.7.', '2018-07-25T00:00:00')
        '2017-07-26T00:00:00'
        >>> normalize_date('26.7,', '2018-07-25T00:00:00')
        '2017-07-26T00:00:00'
        >>> normalize_date('26.7', '2018-07-25T00:00:00')
        '2017-07-26T00:00:00'

    Args:
        date_str (str): format: dd.mm.
        reported_date (str): format: year-mm-dd hh:mm:ss

    Returns:
        str:

    """
    if date_str == '' or date_str == 'IDLE':
        return None

    reported = parse_date(reported_date)
    day, month = [int(each) for each in str(date_str).replace(',', '.').split('.')[:2]]
    year = reported.year

    if month > reported.month:
        year = reported.year - 1

    if month == reported.month:
        if day > reported.day:
            year = reported.year - 1

    return dt.datetime(year, month, day).isoformat()


def normalize_cargoes(item):
    """Normalize cargo.

    Args:
        item (Dict[str, str]):

    Yields:
        List[Cargo] | None

    """
    disch = item['commodity_d']
    load = item['commodity_l']

    if disch != '' and disch not in CARGO_BLACKLIST:
        yield {'product': disch, 'movement': 'discharge'}

    if load != '' and load not in CARGO_BLACKLIST:
        yield {'product': load, 'movement': 'load'}


def get_actual_arrival(berthed_date, arrival_date):
    """When berthed date is before arrival date, the arrival date would be berthed date.

    Args:
        berthed_date (str):
        arrival_date (str):

    Returns:

    """
    berthed = parse_date(berthed_date)
    arrival = parse_date(arrival_date)

    if arrival > berthed:
        return berthed_date

    return arrival_date
