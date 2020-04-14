from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.bill_of_lading import BillOfLading
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


_PLAYER_PREFIXES = ('carrier', 'consignee', 'notify_party', 'shipper')


@validate_item(BillOfLading, normalize=True, strict=False, log_level='error')
def process_item(raw_item, not_terms):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, Any]:

    """
    item = map_keys(raw_item, bol_mapping())

    # discard items that contain irrelevant cargoes
    if not should_keep_item(item['cargo_product'], not_terms):
        return None

    # build Cargo sub-model
    item['cargo'] = {
        'product': item.pop('cargo_product', None),
        'volume': item.pop('cargo_volume_kg', None),
        'volume_unit': Unit.kilogram,
    }

    # build carrier sub-model
    # TODO we should extend our Player model to have these additional fields
    if item.get('carrier_name'):
        item['carrier'] = {
            'name': item.get('carrier_name'),
            'code': item.get('carrier_code'),
            'city': item.get('carrier_city'),
            'state': item.get('carrier_state'),
            'address': item.get('carrier_address'),
            'postcode': item.get('carrier_postcode'),
        }

    # build consignee sub-model
    # TODO we should extend our Player model to have these additional fields
    if item.get('consignee_name'):
        item['consignee'] = {
            'name': item.get('consignee_name'),
            'address': item.get('consignee_address'),
            'postcode': item.get('consignee_postcode'),
        }

    # build shipper sub-model
    # TODO we should extend our Player model to have these additional fields
    if item.get('shipper_name'):
        item['shipper'] = {'name': item.get('shipper_name'), 'address': item.get('shipper_address')}

    # build notify_party sub-model
    # TODO we should extend our Player model to have these additional fields
    if item.get('notify_party'):
        item['notify_party'] = {
            'name': item.get('notify_party_name'),
            'address': item.get('notify_party_address'),
        }

    # remove vestigial intermediary fields
    for field in list(item.keys()):
        if any(field.startswith(prefix) for prefix in _PLAYER_PREFIXES):
            item.pop(field, None)

    # NOTE `not_terms` field is used by the normalisation function but not by the etl
    return item


def bol_mapping():
    return {
        'ARRIVAL DATE': ('arrival_date', lambda x: to_isoformat(clean_value(x), dayfirst=False)),
        'BILL OF LADING': ('bill_of_lading_id', clean_value),
        'CARRIER ADDRESS': ('carrier_address', clean_value),
        'CARRIER CITY': ('carrier_city', clean_value),
        'CARRIER CODE': ('carrier_code', clean_value),
        'CARRIER NAME': ('carrier_name', clean_value),
        'CARRIER STATE': ('carrier_state', clean_value),
        'CARRIER ZIP': ('carrier_postcode', clean_value),
        'CONSIGNEE': ('consignee_name', clean_value),
        'CONSIGNEE ADDRESS': ('consignee_address', clean_value),
        'CONTAINER NUMBER': ignore_key('container identification number'),
        'CONTAINER TYPE': ignore_key('type of container'),
        'COUNTRY OF ORIGIN': ('origin_country', clean_value),
        'DISTRIBUTION PORT': ('distribution_port', clean_value),
        'FOREIGN PORT': ('origin_port', clean_value),
        'GROSS WEIGHT (KG)': (
            'cargo_volume_kg',
            lambda x: clean_value(x).replace(',', '') if clean_value(x) else None,
        ),
        'GROSS WEIGHT (LB)': ignore_key('duplicate of `GROSS WEIGHT (KG)`, except in pounds'),
        'HOUSE VS MASTER': ('house_vs_master', clean_value),
        'IN-BOND ENTRY TYPE': ('in_bond_entry_type', clean_value),
        'MARKS &AMP; NUMBERS': ('marks', clean_value),
        'MASTER B/L': ('master_bill_of_lading_id', clean_value),
        'MEASUREMENT': ignore_key('measurements of containers'),
        'MEASUREMENT UNIT': ignore_key('unit of measurements of containers'),
        'NO. OF CONTAINERS': ignore_key('container count'),
        'NOTIFY ADDRESS': ('notify_party_address', clean_value),
        'NOTIFY PARTY': ('notify_party_name', clean_value),
        'PLACE OF RECEIPT': ('place_of_receipt', clean_value),
        'PRODUCT DESCRIPTION': ('cargo_product', clean_value),
        'provider_name': ('provider_name', None),
        'QUANTITY': ignore_key('quantity of containers'),
        'QUANTITY UNIT': ignore_key('quantity unit of containers'),
        'SEAL': ignore_key('seal value'),
        'SHIP REGISTERED IN': ignore_key('vessel flag'),
        'SHIPPER': ('shipper_name', clean_value),
        'SHIPPER ADDRESS': ('shipper_address', clean_value),
        'US PORT': ('destination_port', clean_value),
        'VESSEL NAME': ('vessel', lambda x: {'name': clean_value(x)}),
        'VOYAGE NUMBER': ('ext_voyage_id', clean_value),
        'ZIP CODE': ('consignee_postcode', clean_value),
    }


def should_keep_item(product, not_terms):
    """ImportGenius search critera are not working properly.

    We re-check not terms from query here to be sure that
     they are not present in the item product description.

    Args:
        item_description (str): item['product']
        query (str): input query

    Returns:
        bool: true if a all not terms are not present in product description

    Examples:
        >>> should_keep_item('CRUDE COCONUT OIL IN BULK', ['coconut'])
        False
        >>> should_keep_item('CRUDE OIL IN BARRELS', ['coconut'])
        True
        >>> should_keep_item('HEATING OIL 45000 BBLS', ['tin'])
        True
        >>> should_keep_item('TIN METAL', ['tin'])
        False

    """
    return not any(e in product.lower().split() for e in not_terms) if product else False


def clean_value(value):
    """Clean raw cell value as obtained from source.

    Args:
        value (str): cell

    Returns:
        str: cleaned cell

    Examples:
        >>> clean_value(' ')
        >>> clean_value('-')
        >>> clean_value('Not Available')
        >>> clean_value('NO MARKS')
        >>> clean_value('legit')
        'legit'
        >>> clean_value('<b> OLIVE</b> <b>OIL</b> FRUITY <b>BLEND</b>')
        'OLIVE OIL FRUITY BLEND'

    """
    value = may_strip(value)
    if not value:
        return None

    if value != '-' and all(s not in value.lower() for s in ('not available', 'no marks')):
        return value.replace('<b>', '').replace('</b>', '').strip()

    return None
