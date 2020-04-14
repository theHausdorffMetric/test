import re

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.services import kp_api
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


KP_API_DATE_PARAM_FORMAT = '%Y-%m-%d'

UNIT_MAPPING = {
    'CBM': Unit.cubic_meter,
    'KB': Unit.kilobarrel,
    'MT': Unit.tons,
    'MB': Unit.megabarrel,
}

PORT_MAPPING = {'F.O': 'OPTIONS', 'TBA': 'OPTIONS', 'FOR ORDERS': 'OPTIONS', 'YANGPU PT': 'YANGPU'}

PORT_ALIASES_MAPPING = {
    'lpg': {
        'CANVEY ISLAND': ['CANVEY ISLAND', 'Canvey Is.'],
        'FAWLEY': ['Fawley Port', 'Fawley', 'Fawley 2'],
        'IMMINGHAM': ['IMMINGHAM'],
        'PEMBROKE': ['PEMBROKE'],
        'SOUTH KILLINGHOLME': ['IMMINGHAM'],
        'TEES': ['North Tees Terminal 1', 'Teesport Petchems Terminal', 'Norsea Oil Terminal'],
    },
    'oil': {
        'FAWLEY': ['Exxon Fawley', 'Southhampton', 'Hamble'],
        'FINNART': ['Finnart'],
        'IMMINGHAM': ['Immingham', 'Tetney'],
        'PEMBROKE': ['Milford Haven', 'Valero Pembroke', 'Milford Haven I', 'Milford Haven II'],
        'TETNEY': ['Tetney', 'Immingham'],
        'TETNEY TERMINAL': ['Tetney', 'Immingham'],
        'TRANMERE': ['Tranmere', 'Liverpool'],
    },
    'cpp': {
        'AVONMOUTH': ['AVONMOUTH', 'BRISTOL', 'ESSO AVONMOUTH'],
        'CANVEY ISLAND': ['Shell Haven', 'Oikos Storage'],
        'DAGENHAM': ['DAGENHAM', 'STOLT DAGENHAM'],
        'FAWLEY': ['Exxon Fawley', 'Southhampton', 'Hamble'],
        'GRAYS': ['GRAYS', 'Greenenergy Storage'],
        'HAMBLE': ['Hamble', 'Southhampton', 'Exxon Fawley'],
        'IMMINGHAM': ['IMMINGHAM'],
        'ISLE OF GRAIN': ['ISLE OF GRAIN'],
        'MILFORD HAVEN': [
            'Milford Haven',
            'Valero Pembroke',
            'Milford Haven I',
            'Milford Haven II',
        ],
        'PEMBROKE': ['Milford Haven', 'Valero Pembroke', 'Milford Haven I', 'Milford Haven II'],
        'PORTBURY': ['Bristol'],
        'PURFLEET': ['Grays'],
        'SHELL HAVEN': ['SHELL HAVEN', 'Oikos Storage'],
        'SOUTH KILLINGHOLME': ['IMMINGHAM'],
        'TEES': ['Teesside', 'seal sands', 'vopak teeside', 'teeside nt4'],
        'THAMES': ['SHELL HAVEN', 'Oikos Storage'],
        'TRANMERE': ['TRANMERE', 'LIVERPOOL'],
        'WEST THURROCK': ['GRAYS', 'Greenenergy Storage'],
    },
}


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, field_mapping())
    platform = item['platform']

    if raw_item['movement'] == 'load':
        processed_item = process_exports_item(item)

    if raw_item['movement'] == 'discharge':
        processed_item = process_imports_item(item)

    # LPG analyst request to change raffinate products in the lpg attachment
    # to show olefins instead, Ilya
    if platform == 'lpg' and 'raffinate' in processed_item['cargo']['product'].lower():
        processed_item['cargo']['product'] = 'olefins'

    return processed_item


def field_mapping():
    return {
        'Berth': ('berth', None),
        'Charterer': ('charterer', lambda x: x if x != 'TBA' else None),
        'Dates': ('lay_can', normalize_lay_can),
        'Grade': ('cargo_product', None),
        # vessel imo may or may not be present for some reports
        'IMO': ('vessel_imo', lambda x: try_apply(x, int, str)),
        'Last Port': ('last_port', normalize_port_name),
        'movement': ('cargo_movement', None),
        'Next Port': ('next_port', normalize_port_name),
        'platform': ('platform', None),
        'Port': ('current_port', normalize_port_name),
        'provider_name': ('provider_name', None),
        'QTY': ('cargo_volume', lambda x: try_apply(x, float, str)),
        'reported_date': ('reported_date', None),
        'Supp/Rcvr': ('seller', lambda x: x if x != 'TBA' else None),
        'Unit': ('cargo_volume_unit', lambda x: UNIT_MAPPING.get(x)),
        'Vessel': ('vessel_name', None),
    }


def normalize_lay_can(raw_date):
    """Normalize raw date string into a lay can date.

    Args:
        raw_date (str): raw date string

    Returns:
        Tuple[str | None, str | None]:

    Examples:
        >>> normalize_lay_can('SLD: 04.06.18')
        '2018-06-04T00:00:00'
        >>> normalize_lay_can('12.06.18 08:00')
        '2018-06-12T00:00:00'
        >>> normalize_lay_can('O/B')

    """
    date_match = re.search(r'\d+\.\d+\.\d+', raw_date)
    if not date_match:
        return None

    return to_isoformat(date_match.group(0), dayfirst=True)


def normalize_port_name(raw_port):
    """Normalize raw port string into a proper port, by removing waypoint indicators.

    Args:
        raw_port (str): raw date string

    Returns:
        Tuple[str | None, str | None]:

    Examples:
        >>> normalize_port_name('GALVESTON')
        'GALVESTON'
        >>> normalize_port_name('GALVESTON VIA CORPUS CHRISTI')
        'GALVESTON'
        >>> normalize_port_name('')
        ''

    """
    port = raw_port.split(' VIA ')[0]
    return PORT_MAPPING.get(port, port)


def process_exports_item(item):
    """Process export spot charters.

    Args:
        item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    return {
        'charterer': item['charterer'],
        'arrival_zone': [item['next_port']],
        'departure_zone': item['current_port'],
        'lay_can_start': item['lay_can'],
        'lay_can_end': item['lay_can'],
        'provider_name': item['provider_name'],
        'reported_date': item['reported_date'],
        'vessel': {'name': item['vessel_name'], 'imo': item.get('vessel_imo')},
        'cargo': {
            'product': item['cargo_product'],
            'volume': item['cargo_volume'],
            'volume_unit': item['cargo_volume_unit'] if item['cargo_volume'] else None,
            'movement': item['cargo_movement'],
        },
        'seller': item['seller'],
    }


def process_imports_item(item):
    """Process import spot charters.

    Because of how spot charters are defined, we cannot use import items directly as a spot charter.
    We need to call the Kpler API to obtain the associated laycan dates with the given discharging
    dates, in order to construct a valid spot charter item.

    Args:
        item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    lay_can_start, lay_can_end = get_imports_lay_can_dates(item)
    return {
        'charterer': item['charterer'],
        'arrival_zone': [item['current_port']],
        'departure_zone': item['last_port'],
        'lay_can_start': lay_can_start,
        'lay_can_end': lay_can_end,
        'provider_name': item['provider_name'],
        'reported_date': item['reported_date'],
        'vessel': {'name': item['vessel_name'], 'imo': item.get('vessel_imo')},
        'cargo': {
            'product': item['cargo_product'],
            'volume': item['cargo_volume'],
            'volume_unit': item['cargo_volume_unit'] if item['cargo_volume'] else None,
            'movement': item['cargo_movement'],
        },
        'seller': item['seller'],
    }


def get_imports_lay_can_dates(item):
    """Get laycan dates for import vessel movements.

    First try to call the API with origin parameter (to improve accuracy).
    If no trades are returned, call the API without the origin parameter.
    Match trades by checking that both destination date and installations are
    accurate to what's stipulated in the report (return the first one that matches).
    Finally, get the lay_can_start and lay_can_end from the final matched trade.

    Args:
        item (Dict[str, str]):

    Returns:
        Tuple[str | None, str | None]:

    """
    if not item['lay_can']:
        return None, None

    import_date = parse_date(item['lay_can'], dayfirst=False)
    # define search timeframe to be 4 months before and 1 month after import date (c.f. analysts)
    # dates are formatted according to Kpler API specification.
    start_date = (import_date - relativedelta(months=4)).strftime(KP_API_DATE_PARAM_FORMAT)
    end_date = (import_date + relativedelta(months=1)).strftime(KP_API_DATE_PARAM_FORMAT)

    # get all trades within timeframe for the vessel with origin parameter
    params = {
        'vessels': item['vessel_name'].lower(),
        'startdate': start_date,
        'enddate': end_date,
        # data source supplies only UK imports
        'zonesdestination': 'united kingdom',
        'zonesorigin': item['last_port'].lower(),
    }
    trades = list(kp_api.get_session(platform=item['platform']).get_trades(params))

    # if no trades matched, relax the search criteria by removing the origin_zone
    if len(trades) == 0:
        params.pop('zonesorigin')
        trades = list(kp_api.get_session(platform=item['platform']).get_trades(params))

    # sanity check, in case we match to an irrelevant port call
    matched_trade = _match_trades(item, trades, import_date)
    if not matched_trade:
        return None, None

    origin_date = parse_date(matched_trade['Date (origin)'], dayfirst=False)
    # lay_can_start is 2 days before origin date, lay_can_end is 1 day after origin date
    lay_can_start = (origin_date - relativedelta(days=2)).isoformat()
    lay_can_end = (origin_date + relativedelta(days=1)).isoformat()
    return lay_can_start, lay_can_end


def _match_trades(item, trades, import_date):
    """Find trade that has accurate destination and date

    For each trade obtained from the API, conduct the following sanity checks (c.f. analysts):
        - trade's destination date is +/- 1 week from the import date stated in the report
        - trade's destination port matches the port stated in the report

    If sanitised successfully, return the matched trade.

    Args:
        trades (Dict[str, str]):
        item (Dict[str, str]):

    Returns:
        Dict[str, str] | None:

    """
    for trade in trades:
        if trade['Date (destination)']:
            destination_date = parse_date(trade['Date (destination)'], dayfirst=False)
            start_date = destination_date - relativedelta(days=7)
            end_date = destination_date + relativedelta(days=7)

            # sanity check if the import_date falls within date range
            if import_date > start_date < end_date:
                # sanity check if port stated in report matches that of trade's destination
                for alias in PORT_ALIASES_MAPPING[item['platform']].get(item['current_port'], []):
                    if trade['Destination'].lower() == alias.lower():
                        return trade

    return None
