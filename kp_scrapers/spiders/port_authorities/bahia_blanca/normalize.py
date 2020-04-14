import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, split_by_delimiters
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.port_authorities.bahia_blanca import parser


logger = logging.getLogger(__name__)

IRRELEVANT_CARGOES = ['CONTENEDORES', 'PROYECTO']

MOVEMENT_MAPPING = {
    'C': 'load',
    'D': 'discharge',
    # NOTE special combination movement where vessel discharges and loads again at the same berth
    'D/C': '_combined',
    'C/D': '_combined',
}


@validate_item(PortCall, normalize=True, strict=False)
def process_forecast_item(raw_item):
    """Transform raw vessel forecast item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, forecast_mapping())

    # discard forecasts without relevant cargo data
    if not item['cargoes']:
        _log_irrelevant_cargo(item)
        return

    # build Vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'length': item.pop('vessel_length')}

    return item


@validate_item(PortCall, normalize=True, strict=False)
def process_harbour_item(raw_item):
    """Transform raw vessel forecast item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, in_harbour_mapping())

    # discard portcall if no cargo listed in the first place
    if not may_strip(item['cargo_product']):
        _log_irrelevant_cargo(item)
        return

    # build Cargoes sub-model
    # multiple products are separated by slashes in this source
    item['cargoes'] = list(
        normalize_cargoes(f'{item.pop("cargo_movement_volume")} {item.pop("cargo_product")}', '/')
    )
    # discard if irrelevant cargoes
    if not item['cargoes']:
        _log_irrelevant_cargo(item)
        return

    # build Vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'length': item.pop('vessel_length')}

    # build proper ETA
    item['berthed'] = to_isoformat(
        f'{item.pop("berthed_date")} {item.pop("berthed_time")}', dayfirst=True
    )

    return item


def forecast_mapping():
    # exhaustive mapping for development/debug clarity
    return {
        '0': ('eta', lambda x: to_isoformat(x, dayfirst=True)),
        '1': ('vessel_name', None),
        '2': ignore_key('vessel flag; could be useful in identifying vessel'),
        '3': ('vessel_length', None),
        '4': ignore_key('shipping agent'),
        '5': ('cargoes', lambda x: list(normalize_cargoes(x, '/'))),
        '6': ignore_key('previous portcall country'),
        '7': ignore_key('current berth, and next portcall country'),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', lambda x: to_isoformat(x.split(' ')[1], dayfirst=True)),
    }


def in_harbour_mapping():
    # exhaustive mapping for development/debug clarity
    return {
        '0': ignore_key('empty'),
        '1': ('berth', None),
        '2': ('vessel_name', None),
        '3': ignore_key('vessel flag; could be useful in identifying vessel'),
        '4': ('vessel_length', None),
        '5': ignore_key('shipping agent'),
        '6': ('cargo_movement_volume', None),
        '7': ('cargo_product', None),
        '8': ignore_key('TODO next portcall country'),
        '9': ignore_key('empty'),
        '10': ('berthed_date', None),
        '11': ('berthed_time', None),
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', normalize_harbour_reported_date),
    }


def normalize_cargoes(raw_cargoes, *delimiters):
    """Normalize a raw cargo string into dicts of cargo movements.

    Examples:  # noqa
        >>> list(normalize_cargoes('D 65.000 CRUDO', '/'))
        [{'movement': 'discharge', 'volume': '65000', 'product': 'CRUDO', 'volume_unit': 'tons'}]
        >>> list(normalize_cargoes('C  13000/9000 PROPANO/BUTANO', '/'))  # doctest: +NORMALIZE_WHITESPACE
        [{'movement': 'load', 'volume': '13000', 'product': 'PROPANO', 'volume_unit': 'tons'},
         {'movement': 'load', 'volume': '9000', 'product': 'BUTANO', 'volume_unit': 'tons'}]
        >>> list(normalize_cargoes('D/C  9.000/10.000 NAFTA/GAS OIL', '/'))  # doctest: +NORMALIZE_WHITESPACE
        [{'movement': 'discharge', 'volume': '9000', 'product': 'NAFTA', 'volume_unit': 'tons'},
         {'movement': 'load', 'volume': '10000', 'product': 'GAS OIL', 'volume_unit': 'tons'}]
        >>> list(normalize_cargoes('C  15.000/10.20 CEBADA/MALTA', '/'))  # doctest: +NORMALIZE_WHITESPACE
        [{'movement': 'load', 'volume': '15000', 'product': 'CEBADA', 'volume_unit': 'tons'},
         {'movement': 'load', 'volume': '10200', 'product': 'MALTA', 'volume_unit': 'tons'}]

    Args:
        raw_cargoes (str):

    Yields:
        Dict[str, str]:

    """
    # sanity check; do not process irrelevant cargoes
    if not any(alias in raw_cargoes for alias in IRRELEVANT_CARGOES):
        cargo = parser._split_movement_volume_product(raw_cargoes)
        # map movement key first; crash if we see an unexpected movement
        cargo['movement'] = MOVEMENT_MAPPING[cargo['movement']]
        # we need to check if the movement combines both load and discharge
        is_combined_movement = cargo['movement'] == '_combined'

        # account for multiple cargoes onboard
        multiples = zip(
            split_by_delimiters(cargo['volume'], *delimiters),
            split_by_delimiters(cargo['product'], *delimiters),
        )
        for idx, (volume, product) in enumerate(multiples):
            # copy original dict to allow correct return values in generator
            cargo = cargo.copy()
            cargo.update(
                product=product, volume=_clean_volume_string(volume), volume_unit=Unit.tons
            )
            # handle case where there is a combination load and discharge movement
            if is_combined_movement:
                cargo.update(movement='discharge' if idx == 0 else 'load')
            yield cargo


def normalize_harbour_reported_date(raw_date):
    """Normalize and cleanup raw reported date from vessels in harbour

    Examples: # noqa
        >>> normalize_harbour_reported_date('08-01-2019HORA:8:00\\nMovimiento (ton)Bahía BlancaPto. RosalesTOTAL')  # doctest: +NORMALIZE_WHITESPACE
        '2019-01-08T08:00:00'

    Args:
        raw_date (str):

    Returns:
        str:

    """
    return to_isoformat(raw_date.split('\n')[0].replace('HORA:', ' '), dayfirst=True)


def _clean_volume_string(raw_volume):
    """Cleanup a raw volume string and normalize to `tons`.

    Source uses full stops as thousands separators.
    There may be a missing zero after the thousands separator.

    Examples:
        >>> _clean_volume_string('34000')
        '34000'
        >>> _clean_volume_string('10.200')
        '10200'
        >>> _clean_volume_string('16.50')
        '16500'

    Args:
        raw_volume (str):

    Returns:
        str:

    """
    if '.' in raw_volume:
        volume, _, remainder = raw_volume.partition('.')
        while len(remainder) < 3:
            remainder += '0'
        return volume + remainder

    return raw_volume


def _log_irrelevant_cargo(item):
    logger.info(f"Vessel {item['vessel_name']} does not contain relevant cargo")
