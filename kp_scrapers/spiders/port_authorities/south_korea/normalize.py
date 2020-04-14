import datetime as dt
import re

from dateutil.parser import parse as parse_dt
import pytz

from kp_scrapers.lib.parser import try_apply
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


# see https://bit.ly/2JXbu3E for exhaustive mapping
PORT_MAPPING = {
    '020': 'Busan',
    '030': 'Incheon',
    '031': 'Pyeongtaek',
    '300': 'Daesan',
    '620': 'Yeosu',
    '621': 'Yeosu',
    '820': 'Ulsan',
}

MOVEMENT_MAPPING = {'1': 'discharge', '2': 'load', 'E': 'load', 'I': 'discharge'}

IRRELEVANT_VESSEL_TYPES = [
    '풀컨테이너선',  # container vessels
    '세미(혼재)컨테이너선',  # mixed-container vessels
    '자동차운반선',  # vehicle carriers
    '여객선',  # passenger ferries
    '일반화물선',  # general cargo vessels
    '냉동.냉장선',  # refridgeration vessels
    '국제카페리',  # car ferries
    '원양 어선',  # fishing vessels
    '산물선(벌크선)',  # bulk carriers
]

VOLUME_UNIT = 'tons'


def field_mapping():
    return {
        'cargoes': ('cargoes', process_cargo),
        # 'cargoes': ('cargoes', lambda x: [Cargo(**map_keys(cargo, cargo_mapping()))
        #                                   for cargo in x if cargo] if x else None),
        'clsgn': ('vessel_callsign', None),
        'etryptDt': ('matching_date', lambda x: dt.datetime.strptime(x, '%Y%m%d%H%M').isoformat()),
        'provider_name': ('provider_name', None),
        'prtAgCd': ('port_name', lambda x: PORT_MAPPING.get(x)),
        # valid imo numbers need to be seven-digit long (as of 18 April 2018)
        'vsslInnb': ('vessel_imo', lambda x: x if try_apply(x, int) and len(x) == 7 else None),
        'vsslKindNm': ('vessel_type', lambda x: None if x in IRRELEVANT_VESSEL_TYPES else x),
        'vsslNm': ('vessel_name', None),
    }


def cargo_mapping():
    return {
        'frghtNm': ('product', smart_split),
        # movement is indicated in last character of manifest number
        'mrnum': ('movement', lambda x: MOVEMENT_MAPPING.get(x[-1])),
        'msrmntUnitSe': ('volume_unit', lambda x: VOLUME_UNIT),
        'tkinSe': ('movement', lambda x: MOVEMENT_MAPPING.get(x)),
        # 'tnspotShapDc': ('product', None),
        'wt': ('volume', lambda x: try_apply(x, float, int, str)),
        'wtTon': ('volume', lambda x: try_apply(x, float, int, str)),
    }


def smart_split(raw_product):
    """Try splitting a raw product string into multiple named entities.

    Examples:
        >>> smart_split('NAPHTHA(26,204), NAPHTHA(50,955-HT')
        ['NAPHTHA(26,204)', 'NAPHTHA(50', '955-HT']
        >>> smart_split('클린디젤(CLEAN DIESEL),MOGAS, JET-A')
        ['클린디젤(CLEAN DIESEL)', 'MOGAS', 'JET-A']
        >>> smart_split('VGO, FO')
        ['VGO', 'FO']
        >>> smart_split('JET A-1, KEROSENE')
        ['JET A-1', 'KEROSENE']

    """
    return re.split(r'[,/]\s*(?![^()]*\))', raw_product)


def process_cargo(raw_cargoes):
    """Split a raw cargo into multiples.

    Examples:  # noqa
        >>> process_cargo([{'mrnum': '19GRTSG357E', 'blNo': 'AB2019V35102', 'msnNo': '0001', 'unityFrghtUpdtOdr': 1, 'prtAgCd': '820', 'vsslInnb': '9711585', 'clsgn': '3FBF6', 'etryptYearCo': '2019-029', 'tkinTkoutSe': 'OO', 'entrpsCd': 'USC0031', 'laidupPlaceCd': 'MBU', 'laidupPlaceSubCd': '11', 'fcltyNm': 'SK1부두 11', 'frghtSe': 'E', 'spprnId': 'GRTS', 'spprnNm': '선사명_함수', 'dmstcOwrCd': None, 'lastPurpsPrtCd': None, 'lastPurpsPrtNm': None, 'shedCd': None, 'shedNm': None, 'blSe': 'S', 'secongNm': 'SK ENERGY', 'secongAddr': 'CO., LTD.', 'secongTelno': None, 'consgeNm': 'TO THE ORDER OF ITOCHU ENEX CO.,LTD.', 'consgeAddr': None, 'consgeTelno': None, 'ntfNm': 'ITOCHU ENEX CO.,LTD.', 'ntfAddr': None, 'ntfTelno': None, 'frghtNm': 'ASPHALT 80/100 PETROLEUM BITUMEN', 'prdlstCd': '271300', 'prdlstNm': '석유코크스·석유역청(瀝靑)과 그 밖의 석유나 역청유(瀝靑油)의 잔재물', 'spclFrghtCd1': None, 'spclFrghtCd2': None, 'spclFrghtCd3': None, 'unno': None, 'unnoNm': None, 'packngKndMapngCd': 'VL', 'packngKndMapngNm': '액체 상태의 벌크', 'lnlMthCd': '3', 'lnlMthNm': '송유관하역', 'lnlEntrpsCd': 'USZ2411', 'lnlEntrpsNm': '협신컴퍼니', 'msrmntUnitSe': 'B', 'msrmnTon': 4068.299, 'wtTon': 674.541, 'msrmntCm': 646.809, 'wtKg': 674541, 'contnCo': 0, 'ldPrtCd': 'KRUSN', 'ldPrtNm': '울산', 'landngPrtCd': 'JPMHR', 'landngPrtNm': 'MIHARA', 'etryndDt': '201905031200', 'vsslNltyCd': 'PA', 'vsslNltyNm': '파나마', 'satmntDt': '201905081745', 'comptAt': 'Y', 'cnnctnId': 'KLGRTSE020', 'bkMsrmntCm': 646.809, 'bkWtKg': 674541, 'unitYong': '1', 'unitBulk': '1'}, {'mrnum': '19GRTSG357E', 'blNo': 'AB2019V35101', 'msnNo': '0001', 'unityFrghtUpdtOdr': 1, 'prtAgCd': '820', 'vsslInnb': '9711585', 'clsgn': '3FBF6', 'etryptYearCo': '2019-029', 'tkinTkoutSe': 'OO', 'entrpsCd': 'USC0031', 'laidupPlaceCd': 'MBU', 'laidupPlaceSubCd': '11', 'fcltyNm': 'SK1부두 11', 'frghtSe': 'E', 'spprnId': 'GRTS', 'spprnNm': '선사명_함수', 'dmstcOwrCd': None, 'lastPurpsPrtCd': None, 'lastPurpsPrtNm': None, 'shedCd': None, 'shedNm': None, 'blSe': 'S', 'secongNm': 'SK ENERGY', 'secongAddr': 'CO., LTD.', 'secongTelno': None, 'consgeNm': 'TO THE ORDER OF ITOCHU ENEX CO.,LTD.', 'consgeAddr': None, 'consgeTelno': None, 'ntfNm': 'ITOCHU ENEX CO.,LTD.', 'ntfAddr': None, 'ntfTelno': None, 'frghtNm': 'ASPHALT 60/80 PETROLEUM BITUMEN', 'prdlstCd': '271300', 'prdlstNm': '석유코크스·석유역청(瀝靑)과 그 밖의 석유나 역청유(瀝靑油)의 잔재물', 'spclFrghtCd1': None, 'spclFrghtCd2': None, 'spclFrghtCd3': None, 'unno': None, 'unnoNm': None, 'packngKndMapngCd': 'VL', 'packngKndMapngNm': '액체 상태의 벌크', 'lnlMthCd': '3', 'lnlMthNm': '송유관하역', 'lnlEntrpsCd': 'USZ2411', 'lnlEntrpsNm': '협신컴퍼니', 'msrmntUnitSe': 'B', 'msrmnTon': 3481.989, 'wtTon': 573.455, 'msrmntCm': 553.593, 'wtKg': 573455, 'contnCo': 0, 'ldPrtCd': 'KRUSN', 'ldPrtNm': '울산', 'landngPrtCd': 'JPMHR', 'landngPrtNm': 'MIHARA', 'etryndDt': '201905031200', 'vsslNltyCd': 'PA', 'vsslNltyNm': '파나마', 'satmntDt': '201905081745', 'comptAt': 'Y', 'cnnctnId': 'KLGRTSE020', 'bkMsrmntCm': 553.593, 'bkWtKg': 573455, 'unitYong': '1', 'unitBulk': '1'}])
        [{'product': 'ASPHALT 80'}, {'product': '100 PETROLEUM BITUMEN'}, {'product': 'ASPHALT 60'}, {'product': '80 PETROLEUM BITUMEN'}]

    """
    to_use = []
    for raw in raw_cargoes:
        cargo = map_keys(raw, cargo_mapping())
        if len(cargo['product']) > 1:
            for product in cargo['product']:
                to_use.append({'product': product})
        else:
            cargo['product'] = cargo['product'][0]
            to_use.append(cargo)
    return to_use


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        ArrivedEvent | EtaEvent:

    """
    item = map_keys(raw_item, field_mapping())
    # discard items with unknown ports and vessel types
    if not item['port_name'] or not item['vessel_type'] or not item['cargoes']:
        return

    # normalize portcall date
    item['reported_date'] = (
        dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
    )
    _pc_date = item.pop('matching_date')
    if parse_dt(_pc_date, dayfirst=False).replace(
        tzinfo=pytz.timezone('Asia/Seoul')
    ) > dt.datetime.utcnow().replace(tzinfo=pytz.timezone('Asia/Seoul')):
        item['arrival'] = _pc_date
    else:
        item['eta'] = _pc_date

    # build vessel sub-model
    item['vessel'] = {
        'name': item.pop('vessel_name'),
        'imo': item.pop('vessel_imo'),
        'call_sign': item.pop('vessel_callsign'),
    }

    # remove rogue fields
    item.pop('vessel_type', None)

    return item
