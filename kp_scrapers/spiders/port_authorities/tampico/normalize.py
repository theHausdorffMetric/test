from datetime import datetime
import logging
import re

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)

PORT_NAME = 'Madero'
PROVIDER_NAME = 'Tampico'

MATCHING_DATE_PATTERN_EXACT = r'(\d{2}/\d{2}/\d{2} \d{2}:\d{2})'
MATCHING_DATE_PATTERN_FUZZY = r'(\d{2}/\d{2})'

SPANISH_TO_MONTH = {
    'enero': 1,
    'febrero': 2,
    'marzo': 3,
    'abril': 4,
    'mayo': 5,
    'junio': 6,
    'julio': 7,
    'agosto': 8,
    'septiembre': 9,
    'setiembre': 9,
    'octubre': 10,
    'noviembre': 11,
    'diciembre': 12,
}

FIELD_MAPS = [
    {
        'cargo_unload': {'volume': 8, 'product': 9},
        'cargo_load': {'volume': 10, 'product': 11},
        'vessel': {'name': 2, 'gt': 3, 'flag': 4, 'length': 5},
        'pa': {'berth': 1, 'shipping_agent': 13},
        'eta': 6,
        'etd': 7,
    },
    {
        'cargo_unload': {'volume': 7, 'product': 8},
        'cargo_load': {'volume': 9, 'product': 10},
        'vessel': {'name': 2, 'gt': 3, 'flag': 4, 'length': 5},
        'pa': {'berth': 1, 'shipping_agent': 11},
        'eta': 6,
    },
    {
        'cargo_unload': {'volume': 6, 'product': 7},
        'cargo_load': {'volume': 8, 'product': 9},
        'vessel': {'name': 2, 'gt': 3},
        'pa': {'berth': 1, 'shipping_agent': 10},
        'eta': 4,
    },
]


def process_cargo(row):
    """Process cargo info given a table row.

    Not all rows contain cargo info however, and so this function is for extracting relevant
    cargo info from each row

    Args:
        row (list): table row as a list, with each element as a table cell

    Returns:
        cargo: Cargo item

    """
    cargo = {}
    section_number = row[-1]
    field_map_section = FIELD_MAPS[section_number]
    # if movement found, append relevant cargo info

    if try_apply(row[field_map_section['cargo_load']['volume']].replace(',', ''), float):
        volume = row[field_map_section['cargo_load']['volume']].replace(',', '')
        product = may_strip(row[field_map_section['cargo_load']['product']])
        movement = 'load'
    elif try_apply(row[field_map_section['cargo_unload']['volume']].replace(',', ''), float):
        volume = row[field_map_section['cargo_unload']['volume']].replace(',', '')
        product = may_strip(row[field_map_section['cargo_unload']['product']])
        movement = 'discharge'
    else:
        return None

    product_list = re.split('[\+\/]', product)
    volume = float(volume) / len(product_list) if len(product_list) > 1 else volume
    for prod in product_list:
        cargo = {
            'product': may_strip(prod),
            'volume': str(volume),
            'volume_unit': Unit.tons,
            'movement': movement,
        }

        yield cargo


def process_matching_date(date_string):
    """Process date string given as ISO timestamp.

    Date strings given may not contain time information, so this function will iterate across
    all possible patterns that have been found in the pdf so far.

    Args:
        date_string (str): date/time string

    Returns:
        str: date/time info in ISO formatted string

    """
    # matching date has both date and time info
    if re.match(MATCHING_DATE_PATTERN_EXACT, date_string):
        return datetime.strptime(date_string, '%d/%m/%y %H:%M').isoformat()

    # matching date only has date info, no time info
    elif date_string.split():
        date_match = re.match(MATCHING_DATE_PATTERN_FUZZY, date_string.split()[0])
        if date_match:
            return (
                datetime.strptime(date_match.group(1), '%d/%m')
                .replace(year=datetime.now().year)
                .isoformat()
            )

    # matching date is missing from the table
    logger.warning(
        'no matching date found to match regex patterns {}, {}'.format(
            MATCHING_DATE_PATTERN_EXACT, MATCHING_DATE_PATTERN_FUZZY
        )
    )
    return None


def normalize_reported_date(raw_date):
    """Normalize reported date.

    Args:
        raw_date (str):

    Returns:
        str: reported date in ISO-8601 format

    Examples:
        >>> normalize_reported_date('POSICIONDEBUQUESDELDIA18/04/2018alas10:16')
        '2018-04-18T10:16:00'

    """
    matches = re.search(r'(\d{1,2}\/\d{1,2}\/\d{4})[a-z]*(\d{1,2}:\d{2})', raw_date).groups()
    return to_isoformat(' '.join(matches), dayfirst=True)


@validate_item(PortCall, normalize=True, strict=False)
def process_item(**parsed_data):
    """Transform into a usable event.
    """
    row = parsed_data['row']
    # no cargo info associated with vessel
    if process_cargo(row):
        cargo = list(process_cargo(row))
    else:
        return

    # section number is appended at the end of the list, as per InformationParser
    section_number = row[-1]
    try:
        matching_date = process_matching_date(row[FIELD_MAPS[section_number]['eta']])
    except AttributeError:
        logger.info("Eta doesn't match")
        return None

    event_type = ''
    if section_number == 0:
        event_type = 'arrival'
    elif section_number == 1:
        event_type = 'eta'
    elif section_number == 2:
        if row[FIELD_MAPS[section_number]['pa']['berth']] != '':
            event_type = 'berthed'
        else:
            event_type = 'arrival'

    item = {}
    # append pa event details
    field_map = FIELD_MAPS[section_number]['pa']
    for field_name in field_map:
        item[field_name] = row[FIELD_MAPS[section_number]['pa'][field_name]]
    item['port_name'] = PORT_NAME
    item['provider_name'] = PROVIDER_NAME
    item[event_type] = matching_date
    item['reported_date'] = normalize_reported_date(parsed_data['reported_date'])
    # append cargo/vessel info
    item['cargoes'] = cargo
    vessel = {}
    for field_name in FIELD_MAPS[section_number]['vessel']:
        vessel[field_name] = row[FIELD_MAPS[section_number]['vessel'][field_name]]

    item['vessel'] = {
        'name': vessel.get('name'),
        'gross_tonnage': vessel.get('gt').replace(',', ''),
        'flag_name': vessel.get('flag'),
        'length': try_apply(vessel.get('length'), float, int),
    }

    if not item['vessel']['name'] or not item['cargoes']:
        return None
    else:
        return item
