import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.unlocode import get_location
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.port_authorities.peru.constants import PRODUCT_BLACKLIST


logger = logging.getLogger(__name__)


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into something usable.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, portcall_mapping())

    # discard portcalls without cargo information
    if not item['cargoes']:
        logger.info(f"Vessel {item['vessel_name']} did not provide relevant cargo details, discard")
        return

    # discard manifests with more than one associated portcall
    # NOTE because the source does not provide separate ETAs for multiple portcalls, we cannot
    # make sense of such cases; discard
    if len(item['port_names']) > 1:
        logger.warning(f"Manifest has more than one associated portcall, unable to process")
        return

    # build proper port_name
    port = get_location(item.pop('port_names').pop())
    if not port:
        logger.info(f"Vessel {item['vessel_name']} has an unknown destination, discarding")
        return
    item['port_name'] = port.split(',')[0]

    # build vessel sub-model
    item['vessel'] = {'name': item.pop('vessel_name'), 'flag_code': item.pop('vessel_flag')}

    return item


def portcall_mapping():
    return {
        'cargoes': ('cargoes', lambda x: process_cargo(x)),
        'Empresa de Transporte': ignore_key('shipping agent'),
        'Fecha de Descarga:': ignore_key('departure date'),
        'Fecha de Llegada:': ('eta', lambda x: to_isoformat(x, dayfirst=True)),
        'Manifiesto': ignore_key('manifest number'),
        'Matr�cula de la Nave': ('vessel_name', None),
        'Matrícula de la Nave': ('vessel_name', None),
        'Nacionalidad:': ('vessel_flag', None),
        'No Bultos:': ignore_key('irrelevant'),
        'No Detalles:': ignore_key('unknown'),
        'P.Bruto:': ignore_key('unknown'),
        'port_names': ('port_names', None),
        'provider_name': ('provider_name', None),
        'raw_cargoes': ignore_key('raw cargo; not needed anymore'),
        'reported_date': ('reported_date', lambda x: to_isoformat(x, dayfirst=True)),
    }


def cargo_mapping():
    return {
        'Bultos': ('volume_unit', lambda _: Unit.tons),
        'Consignatario': ignore_key('consignee'),
        'Descripción de Mercadería': ('product', filter_product),
        'Embarcador': ignore_key('wharf'),
        'Marcas y Números': ignore_key('remarks'),
        'Peso Bruto': ('volume', lambda x: str(int(int(x) / 1000))),
    }


def process_cargo(raw_cargoes):
    """Transform raw cargo into a usable Cargo model.

    Args:
        raw_cargoes (List[Dict[str, str]]):

    Returns:
        List[Dict[str, str]]:

    """
    res = []
    for raw_cargo in raw_cargoes:
        cargo = map_keys(raw_cargo, cargo_mapping())
        if cargo.get('product') is None:
            # terminate early, we know the rest of the cargoes is irrelevant if one is
            return []

        # for this source, a manifest is only created for importing cargo, not exporting
        # hence if a cargo is present, it must be a discharge by default
        cargo.update(movement='discharge')
        res.append(cargo)

    return res


def filter_product(raw_product):
    """Filter products and keep only relevant ones.

    Args:
        raw_product (str):

    Returns:
        str | None: None if product is irrelevant

    """
    if any(alias in raw_product.lower() for alias in PRODUCT_BLACKLIST):
        return None

    return may_strip(raw_product)
