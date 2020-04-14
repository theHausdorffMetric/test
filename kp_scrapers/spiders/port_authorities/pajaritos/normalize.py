import datetime as dt
import logging

from dateutil.parser import parse as parse_date_str

from kp_scrapers.lib.parser import may_strip, split_by_delimiters
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

SPANISH_MONTH_ABBREVIATION = {
    'ene': '1',
    'feb': '2',
    'mar': '3',
    'abr': '4',
    'may': '5',
    'jun': '6',
    'jul': '7',
    'ago': '8',
    'sep': '9',
    'oct': '10',
    'nov': '11',
    'dic': '12',
}

IRRELEVANT_PRODUCTS = [
    'APOYO',
    'CONTENEDOR',
    'INSTRUCCION',
    'LLEVA A LA TIRA',
    'MANTENIMIENTO',
    'UNIDAD',
]


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        ArrivedEvent | DepartedEvent | EtaEvent:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    # discard items without relevant cargo
    raw_product = item.pop('cargo_product', None)
    raw_movement = item.pop('cargo_movement', None)
    if not raw_product or any(irr in raw_product for irr in IRRELEVANT_PRODUCTS):
        logger.warning(f'Discarding vessel {item["vessel_name"]}: irrelevant cargo {raw_product}')
        return

    # build Cargo sub-model
    item['cargoes'] = [
        {'product': product, 'movement': raw_movement}
        for product in split_by_delimiters(raw_product, '/')
    ]

    # build Vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'gross_tonnage': item.pop('vessel_gt', None),
        'length': item.pop('vessel_length', None),
    }

    # check if a portcall-related date is given, discard if not
    _eta_date = item.pop('eta_date', None)
    _eta_time = item.pop('eta_time', None)
    if not _eta_date:
        logger.info(f'Discarding vessel {item["vessel"]["name"]}: no ETA provided')
        return

    # normalize Arrived/Departed dates
    _arr_dep = (parse_date_str(item['reported_date']) - dt.timedelta(days=1)).isoformat()
    if 'FONDEO' in _eta_date or 'FONDEADO' in _eta_date:
        item['arrival'] = _arr_dep
        return item
    if 'PTO' in _eta_date:
        item['departure'] = _arr_dep
        return item

    if normalize_eta_date(_eta_date):
        # normalize ETA dates
        item['eta'] = dt.datetime.combine(
            normalize_eta_date(_eta_date), normalize_eta_time(_eta_time)
        ).isoformat()
    else:
        item['eta'] = None

    return item


def field_mapping():
    return {
        'AGENTE NAVIERO': ignore_key('shipping agent'),
        'BANDERA': ignore_key('vessel flag'),
        'CALADOS': ignore_key('vessel draught'),
        'CARGA': ('cargo_movement', lambda x: 'load' if x else 'discharge'),
        'DESCARGA': ignore_key('discharge cargo movement'),
        'ESLORA': (
            'vessel_length',
            lambda x: may_strip(x.replace('?', '')).split('\'')[0] if x else None,
        ),
        'FECHA': ('eta_date', lambda x: x.replace('?', '') if x else None),
        'HORA': ('eta_time', None),
        'MUELLE': ignore_key('berth'),
        'NOMBRE DEL BUQUE': ('vessel_name', lambda x: may_strip(x.replace('?', '')) if x else None),
        'ORIGEN': ignore_key('previous_zone'),
        'port_name': ('port_name', None),
        'PRODUCTO': ('cargo_product', lambda x: may_strip(x.replace('?', '')) if x else None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'T.R.B.': ('vessel_gt', lambda x: x.replace(',', '') if x else None),
    }


def normalize_eta_date(raw_date):
    """Normalize ETA date from raw string.

    Because no year data is present in the table, it needs to be inferred indirectly.
    This can be an issue during the period where we rollover to a new year.
    To resolve this, we check for if the ETAs are older than 12 weeks.
    If so, we add one year to the ETA date.

    Dates can come in one or 3 formats:
        - "ABRIL.05"
        - "ABR.05"
        - "ABRIL"

    Args:
        raw_date (str):

    Returns:
        datetime.datetime | None:

    """
    # replace spanish months with english months
    for spanish_month, month_value in SPANISH_MONTH_ABBREVIATION.items():
        if spanish_month in raw_date.lower():
            if len(raw_date.split('.')) == 2:
                month, day = raw_date.split('.')
                month = month_value

            else:
                return None

    # sanity check if ETA is in the past (i.e., older than 12 weeks)
    date_object = parse_date_str('-'.join([month, day]), dayfirst=False)
    if date_object < dt.datetime.now() - dt.timedelta(weeks=12):
        date_object = date_object.replace(year=date_object.year + 1)

    return date_object


def normalize_eta_time(raw_time):
    """Normalize ETA time from raw string.

    Because no year data is present in the table, it needs to be inferred indirectly.
    This can be an issue during the period where we rollover to a new year.
    To resolve this, we check for if the ETAs are older than 12 weeks.
    If so, we add one year to the ETA date.

    Args:
        raw_time (str | None):

    Returns:
        datetime.time:

    """
    if not raw_time:
        return dt.time(hour=0)
    if raw_time == 'AM':
        return dt.time(hour=6)
    if raw_time == 'PM':
        return dt.time(hour=18)
    if raw_time == 'AM/PM':
        return dt.time(hour=12)

    return dt.time(hour=0)


def normalize_cargo(raw_product, movement):
    """Normalize ETA time from 'load' and 'discharge' keys in item.

    Args:
        item (Dict[str, str]):

    Yields:
        Cargo:

    """
    if not raw_product or any(irrelevant in raw_product for irrelevant in IRRELEVANT_PRODUCTS):
        logger.info(f'Discarding irrelevant product: {raw_product}')
        return

    for product in split_by_delimiters(raw_product, '/'):
        yield {'product': product, 'movement': movement}
