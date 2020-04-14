from datetime import datetime

from kp_scrapers.lib.date import ISO8601_FORMAT, may_parse_date_str, to_isoformat
from kp_scrapers.lib.utils import ignore_key, map_keys


MOVEMENT_MAPPING = {'Loading': 'load', 'Offload': 'discharge', 'DISCH': 'discharge'}


def process_item(raw_item):
    """Transform raw item to Portcall model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, field_mapping())

    # cargo sub model
    cargo_info = item.pop('movement', None)
    if cargo_info:
        # In some cases, product and movement are combined as a single string at the source.
        movement, _, product = cargo_info.partition(' ')
        if not product:
            item['cargoes'] = {'product': movement}
        else:
            item['cargoes'] = {
                'product': product,
                'movement': MOVEMENT_MAPPING.get(movement, movement.lower()),
            }

    # check for already arrived vessel, if the vessel is already anchored choose the reported_date
    # as eta
    if 'anchor' in item.get('eta').lower() or 'port' in item.get('eta').lower():
        item['eta'] = item.get('reported_date')
    else:
        try:
            item['eta'] = to_isoformat(item.pop('eta'), dayfirst=True)
        except ValueError:
            # Few cases are having string values in the field.
            item['eta'] = None

    # check whether ETD year is a valid year if not update it with the current year.
    try:
        if item.get('departure'):
            departure = may_parse_date_str(item.get('departure'), ISO8601_FORMAT)
            # In some cases, the departure year is not formatted correctly in the pdf.
            # below condition will ensure correct year is derived under such circumstances.
            if departure.year < (datetime.now().year) - 1:
                item['departure'] = departure.replace(year=datetime.now().year)
    except ValueError:
        # Few cases are having string values in the field.
        pass

    return item


def field_mapping():
    return {
        'VESSEL': ('vessel', lambda x: {'name': x}),
        'ETA': ('eta', None),
        'ETB': ('berthed', lambda x: to_isoformat(x, dayfirst=True)),
        'ETD': ('departure', lambda x: to_isoformat(x, dayfirst=True)),
        'OPERATIONS': ('movement', None),
        'BERTH': ('berth', None),
        'REMARKS': ignore_key('REMARKS'),
        'PORT STAY': ignore_key('PORT STAY'),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
        'provider_name': ('provider_name', None),
        'port_name': ('port_name', None),
        'OPERATION': ('movement', None),
    }
