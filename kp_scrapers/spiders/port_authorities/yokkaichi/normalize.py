import datetime as dt

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from kp_scrapers.lib.parser import may_apply, may_strip
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.port_call import PortCall
from kp_scrapers.models.utils import validate_item


# mininum gross tonnage before we can consider yielding data from this source
MIN_GROSS_TONNAGE = 1000

VESSEL_MAPPING = {
    'ゼクリート': 'Zekreet',
    'ドーハ': 'Doha',
    'アル ワックラ': 'Al Wakrah',
    '泉州丸': 'Senshu Maru',
    'アル　ズバーラ': 'Al Zubarah',
    'アルジャスラ': 'Al Jasrah',
    'アル　ワヂバ': 'Al Wajbah',
    'アルビダ': 'Al Bidda',
    'シズキサン': 'Shizukisan',
    'カミネサン': 'Kaminesen',
    '元栄': 'Gen Ei',
}

ZONE_MAPPING = {
    'フジャイラ': 'Fujairah',
    'ウルサン': 'Ulsan',
    'ｼﾝｶﾞﾎﾟｰﾙ': 'Singapore',
    'ﾗｽﾗﾌｧﾝ': 'Ras Laffan',
    'ボンタン': 'Bontang',
    'ﾀﾞｽｱｲﾗﾝﾄﾞ': 'Das Island',
    '伊万里': 'Imari',
    '泉北': 'Senboku',
    '未定': None,  # means TBD
    '千葉': 'Chiba',
    'プサン': 'Busan',
    '神戸': 'Kobe',
    'ペルシャ湾': 'Persian Gulf',
    'ﾋﾞﾝﾄｩｰﾙｰ': 'Bintulu',
    'ﾋｭｰｽﾄﾝ': 'Houston',
    '鹿島': 'Kashima',
    '名古屋': 'Nagoya',
    '堺': 'Sakai',
    '川崎': 'Kawasaki',
    '衣浦': 'Kinuura',
    '水島': 'Mizushima',
    'ﾐﾅｱﾙｱﾏﾃﾞｨ': 'Mina al Ahmadi',
    'ジャイマー': 'Juaymah',
    'ヨース': 'Yeosu',
    'ﾉﾎﾞﾛｼｽｸ': 'Novorossiysk',
    '波方 ': 'Namikata',
    '横浜': 'Yokohama',
    'ボニー': 'Bonny',
    'ラスタヌラ': 'Ras Tanura',
    'ｼﾞﾙｸｱｲﾗﾝﾄﾞ': 'Zirku',
    '長崎': 'Nagasaki',
    'ﾊｳﾙﾌｧｶﾝ': 'Khor Fakkan',
    'ｳｲｽﾞﾈﾙﾍﾞｲ': 'Whitney Bay',
    'ｻﾘﾅｸﾙｽ ': 'Salina Cruz',
}

INSTALLATION_MAPPING = {'ＣＳＢ': 'Cosmo Yokkaichi', 'ＳＳＢ': 'Showa Yokkaichi'}

PRODUCT_MAPPING = {
    # gas
    'ＬＮＧ': 'LNG',
    'ＬＰＧ': 'LPG',
    # liquids
    '原油': 'crude oil',
    '重油': 'fuel oil',
    '軽油': 'diesel fuel',
    'ガスオイル': 'gasoil',
    'ガソリン': 'gasoline',
    '基油': 'base oil',
    '潤滑油': 'lube oil',
    'ナフサ': 'naphtha',
    'メタノール': 'methanol',
    'エタノール': 'ethanol',
    'カノーラ': 'canola oil',
    'ナタネ粕': 'rapeseed oil',
    'プロピレン': 'propylene',
    'キシレン': 'xylene',
    'キュメン': 'cumeme',
    'ブタン': 'butane',
    'ペンタン': 'pentane',
    'ブタジオール': 'butanediol',
    'オクチル酸': 'octanoic acid',
    'アクリル酸': 'acrylic acid',
    'ｽﾁﾚﾝﾓﾉﾏｰ': 'styrene monomer',
    'エチルヘキサノール': 'ethylhexanol',
    '2-ｴﾁﾙﾍｷｼﾙｱﾙｺｰﾙ': '2-Ethylhexanol',
    '2ｴﾁﾙﾍｷｻﾉｰﾙ': '2-Ethylhexanol',
    '過酸化水素': 'hydrogen peroxide',
    # dry bulk
    '石炭': 'coal',
    'コークス': 'coke',
    'ペットコークス': 'petcoke',
    'オイルコークス': 'graphitized petcoke',
    'ソーダ灰': 'soda ash',
    '苛性カリ': 'caustic potash',
    '溶融硫黄': 'molten sulfur',
    'サルファ': 'sulfur',
    'イルミナイト': 'aluminite',
    'チタンスラグ': 'titanium slag',
    '製鋼スラグ': 'steel slag',
    '硫安': 'ammonium sulfate',
    '混和剤': 'concrete admixture',
    'ベントナイト': 'bentonite',
    '銅板': 'copper plates',
    '電気銅': 'copper',
    'ジルコン': 'zircon',
    '硅石': 'silica',
    '鋼材': 'steel',
    '鉄鋼スラグ': 'steel slag',
    '巣鉛': 'lead',
    'パイプ': 'pipes',
    'スチールコイル': 'coils',
    'コイル': 'coils',
    'クリンカー': 'clinkers',
    '石膏': 'plaster',
    '合板': 'plywood',
    'ラテックス': 'latex',
    '石灰石': 'limestone',
    '砂': 'sand',
    '塩': 'salt',
    '工業塩': 'salt',
    '食塩': 'salt',
    'タルク': 'talc',
    'スオイル': 'soil',
    'グルテンフィード': 'gluten feed',
    'コーン': 'corn',
    '北麦': 'barley',
    '小麦': 'wheat',
}


PRODUCT_BLACKLIST = [
    'コンテナ',  # container
    '車',  # car
    '機械',  # mechanical equipment
    'プラント部材',  # mechanical equipment
    '建設用資材',  # construction materials
]

MOVEMENT_MAPPING = {'揚': 'discharge', '積': 'load'}


@validate_item(PortCall, normalize=True, strict=False)
def process_item(raw_item):
    """Map and normalize raw_item into a usable event.

    Args:
        raw_item (dict[str, str]):

    Returns:
        Dict:

    """
    item = map_keys(raw_item, portcall_mapping(), skip_missing=True)
    vessel_eta_departure_agent = item.pop('vessel_eta_departure_agent')

    # discard portcall if no relevant cargoes onboard
    if not item.get('cargoes'):
        return

    # build Vessel sub-model
    item['vessel'] = {
        'name': vessel_eta_departure_agent['vessel_name'],
        **item.pop('vessel_attributes', {}),
    }

    # discard vessels if they are below a certain gross tonnage
    if 0 < item['vessel'].get('gross_tonnage', -1) <= MIN_GROSS_TONNAGE:
        return

    # build proper portcall dates
    item['eta'], item['departure'] = normalize_portcall_dates(
        item['reported_date'],
        vessel_eta_departure_agent['eta'],
        vessel_eta_departure_agent['departure'],
    )

    # map proper installation
    item['installation'] = INSTALLATION_MAPPING.get(item.get('berth'))

    return item


def portcall_mapping():
    return {
        # vessel_name, nationality, agent, arrival, departure
        '船名 / 国籍 / 代理店 / 入港日時 / 出港日時': (
            'vessel_eta_departure_agent',
            normalize_vessel_name_agent_eta,
        ),
        # berth_to, shore_side, bow_line_position, stern_line_position, vessel_info
        'バース / 着岸舷側 / 船首綱位置 / 船尾綱位置': ('berth', lambda x: x[0] if x else None),
        # gross_tonnage, length, dead_weight, draught, b.b. ???
        '総トン数 / LOA / DWT / 吃水 / B.B': ('vessel_attributes', normalize_vessel_attributes),
        # discharge, load
        '揚荷貨物 / 積荷貨物': ('cargoes', normalize_cargoes),
        # departure_port, destination_port, front_port, next_port, remarks
        '1:仕出港 / 2:仕向港 / 3:前港 / 4:次港': ignore_key('TODO scrape next_zone for future ETAs'),
        # in-port movement timings
        'シフト情報 / 備考': ignore_key('irrelevant'),
        # meta info
        'port_name': ('port_name', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_cargoes(raw):
    """Normalize cargo.

    Examples:
        >>> normalize_cargoes(["揚）", "積）石炭"])
        [{'movement': 'load', 'product': 'coal'}]
        >>> normalize_cargoes(["揚）コンテナ", "積）コンテナ"])

    """
    cargoes = []
    for cargo in raw:
        if MOVEMENT_MAPPING.get(cargo[0] if cargo else ''):
            product = may_strip(cargo.partition('）')[2])
            if product and product not in PRODUCT_BLACKLIST:
                cargoes.append(
                    {
                        'movement': MOVEMENT_MAPPING[cargo[0]],
                        'product': PRODUCT_MAPPING.get(product, product),
                    }
                )

    return cargoes if cargoes else None


def normalize_vessel_name_agent_eta(raw):
    """Extract and normalize vessel name, eta, agent fields.

    All fields will be given by the source, so no need to account for missing fields.

    Examples:
        >>> normalize_vessel_name_agent_eta(["神正丸", "日本", "名古屋平水", "23-1140", "23-1600"])
        {'vessel_name': '神正丸', 'eta': '23-1140', 'departure': '23-1600', 'shipping_agent': '名古屋平水'}

    """
    # don't need vessel flag for now
    vessel_name, _, shipping_agent, eta, departure = raw
    return {
        'vessel_name': VESSEL_MAPPING.get(vessel_name, vessel_name),
        'eta': eta,
        'departure': departure,
        'shipping_agent': shipping_agent,
    }


def normalize_vessel_attributes(raw):
    """Normalize vessel information, to update vessel model.

    Examples:
        >>> normalize_vessel_attributes(['32,431.00 総ト', '189.99 LOA', '57,763.00 DWT', '11.99 吃水'])
        {'gross_tonnage': 32431, 'length': 189, 'dead_weight': 57763}

    """
    res = {}
    for attr in raw:
        if '総ト' in attr:
            res.update(gross_tonnage=_normalize_numeric(attr))
        elif 'LOA' in attr:
            res.update(length=_normalize_numeric(attr))
        elif 'DWT' in attr:
            res.update(dead_weight=_normalize_numeric(attr))

    return res


def normalize_portcall_dates(reported_date, arrival, departure):
    """Parse eta/arrival and departure date with reported date as reference.

    Examples:
        >>> normalize_portcall_dates('2019-05-27', '29-1800', '02-0300')
        ('2019-05-29T18:00:00', '2019-06-02T03:00:00')

    """
    day_arrival, time_arrival = arrival.split('-')
    day_departure, time_departure = departure.split('-')
    reported_date = parse_date(reported_date, dayfirst=False)

    arrival = _format_date(reported_date, day_arrival, time_arrival)
    departure = _format_date(reported_date, day_departure, time_departure)

    # account for month rollover scenarios
    if departure < arrival:
        departure += relativedelta(months=1)

    return arrival.isoformat(), departure.isoformat()


def _normalize_numeric(raw_data):
    """Normalize numeric fields.

    Examples:
        >>> _normalize_numeric('32,431.00 総ト')
        32431
        >>> _normalize_numeric('189.99 LOA')
        189

    """
    return may_apply(raw_data.split()[0].replace(',', ''), float, int)


def _format_date(reported, day=None, time=None):
    return dt.datetime(
        year=reported.year,
        month=reported.month,
        day=int(day) if day else reported.day,
        hour=int(time[:2]) if time else reported.hour,
        minute=int(time[2::]) if time else reported.minute,
    )
