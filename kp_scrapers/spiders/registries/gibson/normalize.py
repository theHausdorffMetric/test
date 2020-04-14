from datetime import datetime
import logging

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.parser import may_strip, try_apply
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.models.utils import validate_item
from kp_scrapers.models.vessel import PlayerRole, VesselRegistry, VesselStatus
from kp_scrapers.spiders.registries.gibson import constants


logger = logging.getLogger(__name__)

STATUS_DETAIL_MAPPING = {
    'Canc': VesselStatus.cancelled_order,
    'Conv': VesselStatus.converting,
    'Demo': VesselStatus.broken_up,
    'LAID': VesselStatus.laid_up,
    'LOI': VesselStatus.unknown,
    'Lost': VesselStatus.total_loss,
    'NRpt': VesselStatus.unknown,
    'NVE': VesselStatus.unknown,
    'Pend': VesselStatus.unknown,
    'Stor': VesselStatus.in_service,  # they are active FPSOs/FSOs
    'Trdg': VesselStatus.in_service,
    'Newb': VesselStatus.on_order,
}

TYPE_SUBTYPE_MAPPING = {
    # lng platform
    ('LNG', None): 'LNG Tanker',
    ('LNG', 'CNG'): 'LNG Tanker',
    ('LNG', 'FLNG'): 'LNG Tanker',
    ('LNG', 'FSRU'): 'LNG Tanker',
    ('LNG', 'FSU'): 'LNG Tanker',
    ('LNG', 'LNGBU'): 'LNG Tanker',
    ('LNG', 'REGAS'): 'LNG Tanker',
    ('Offs', 'FSRU'): 'LNG Tanker',
    # lpg platform
    ('LPG', None): 'LPG Tanker',
    ('LPG', 'PRESS'): 'LPG Tanker',
    ('LPG', 'REF'): 'LPG Tanker',
    ('LPG', 'VLEC'): 'LPG Tanker',
    ('LPG', 'SEMI.'): 'LPG Tanker',
    # dpp platform
    ('Chem', 'ASBIT'): 'Asphalt/Bitumen Carrier',
    ('Chem', 'ASPH'): 'Asphalt Carrier',
    ('Tank', 'ASBIT'): 'Asphalt/Bitumen Carrier',
    ('Tank', 'ASPH'): 'Asphalt Carrier',
    ('Tank', 'BIT'): 'Bitumen Carrier',
    # oil/cpp platform
    # ('Chem', None): 'TODO unsure',
    ('Chem', 'ACID'): 'Chemical/Oil Products Tanker',
    ('Chem', 'CHEM'): 'Chemical/Oil Products Tanker',
    ('Chem', 'MOLT'): 'Products Tanker',
    ('Chem', 'VEG'): 'Oil Products Tanker',
    ('Offs', 'FPSO'): 'FSO, Oil',
    ('Offs', 'FSO'): 'FSO, Oil',
    ('Tank', 'BUNKE'): 'Oil Products Tanker',
    ('Tank', 'FPSO'): 'FSO, Oil',
    ('Tank', 'FSO'): 'FSO, Oil',
    ('Tank', 'FSU'): 'Crude Oil Tanker',
    ('Tank', 'OIL'): 'Crude Oil Tanker',
    ('Tank', 'MOLT'): 'Products Tanker',
    ('Tank', 'PROD'): 'Oil Products Tanker',
    ('Tank', 'VEG'): 'Oil Products Tanker',
    # oil/cpp/coal platform
    ('Comb', 'O/O'): 'Ore/Oil Carrier',
    ('Comb', 'OBO'): 'Ore/Oil Carrier',
    # coal platform
    ('Bulk', None): 'Bulk Carrier',
    ('Bulk', 'CABU'): 'Bulk/Caustic Soda Carrier (CABU)',
    ('Bulk', 'CEMNT'): 'Bulk Carrier',
    ('Bulk', 'CHIP'): 'Bulk Carrier',
    ('Bulk', 'COAL'): 'Bulk Carrier',
    ('Bulk', 'LAKER'): 'Bulk Carrier',
    ('Bulk', 'Lime'): 'Bulk Carrier',
    ('Bulk', 'LOG'): 'Bulk Carrier',
    ('Bulk', 'LUMBE'): 'Bulk Carrier',
    ('Bulk', 'ORE'): 'Ore Carrier',
    ('Bulk', 'SDISC'): 'Self-Discharging Bulk Carrier',
    ('Bulk', 'Wpulp'): 'Bulk Carrier',
    ('Chem', 'CAUST'): 'Bulk/Caustic Soda Carrier (CABU)',
    ('Gen', 'GENRO'): 'General Cargo Ship (with Ro-Ro facility)',
    ('Gen', 'H.LIF'): 'Heavy Load Carrier',
    ('Gen', 'H.LSS'): 'Heavy Load Carrier, semi submersible',
    ('Gen', 'SDISC'): 'Self-Discharging Bulk Carrier',
    ('Misc', 'TSV'): 'Trans Shipment Vessel',
}


# these vessels are marked differently by Gibson than their actual type
# overrides default type/subtype mapping above
OVERRIDE_TYPE_MAPPING = {
    '9230933': 'FSO, Oil',  # LIBERDADE
    '9685425': 'Combination Gas Tanker (LNG/LPG)',  # JS INEOS INSIGHT
    '9685437': 'Combination Gas Tanker (LNG/LPG)',  # JS INEOS INGENUITY
    '9685449': 'Combination Gas Tanker (LNG/LPG)',  # JS INEOS INTREPID
    '9685451': 'Combination Gas Tanker (LNG/LPG)',  # JS INEOS INSPIRATION
    '9744958': 'Combination Gas Tanker (LNG/LPG)',  # JS INEOS INNOVATION
    '9744960': 'Combination Gas Tanker (LNG/LPG)',  # JS INEOS INDEPENDENCE
    '9771511': 'Combination Gas Tanker (LNG/LPG)',  # JS INEOS INVENTION
    '9771523': 'Combination Gas Tanker (LNG/LPG)',  # JS INEOS INTUITION
}

COMPLIANCE_MAPPING = {
    'scrubber_fitted': 'Scrubber',
    'scrubber_ready': 'Scrubber Ready',
    'scrubber_planned': 'Scrubber Planned',
}


@validate_item(VesselRegistry, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item into a usable event.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str, str]:

    """
    item = map_keys(raw_item, vessel_mapping())

    # discard vessels without proper IMO numbers
    if not item['imo']:
        return

    # map proper vessel type
    raw_type, raw_subtype = item.pop('vessel_type'), item.pop('vessel_subtype')
    item['type'] = TYPE_SUBTYPE_MAPPING.get((raw_type, raw_subtype))
    if not item['type']:
        logger.info(
            f'Vessel {item["name"]} has unmapped type ({raw_type}, {raw_subtype}), discarding'
        )
        return

    # override default type mapping for some vessels
    item['type'] = OVERRIDE_TYPE_MAPPING.get(item['imo'], item['type'])

    # map proper vessel status
    item['status'] = STATUS_DETAIL_MAPPING.get(item.pop('status'))
    if not item['status']:
        return

    # map proper vessel flag
    item['flag_code'], item['flag_name'] = item.pop('flag', (None, None))

    # build Player sub-model
    _build_players(item)

    # _detect compliance method
    item['compliance_method'] = _detect_compliance(item)

    # build date field
    item['build_at'] = _build_date(
        item.get('build_day'), item.get('build_month'), item.get('build_year')
    )
    item['order_at'] = _build_date(
        item.get('order_day'), item.get('order_month'), item.get('order_year')
    )
    item['keel_laid_at'] = _build_date(
        item.get('keel_laid_day'), item.get('keel_laid_month'), item.get('keel_laid_year')
    )
    item['launch_at'] = _build_date(
        item.get('launch_day'), item.get('launch_month'), item.get('launch_year')
    )
    item['dead_at'] = _build_date(
        item.get('dead_day'), item.get('dead_month'), item.get('dead_year')
    )

    item['raw_date'] = _build_raw_date(item)

    return item


def _build_date(day, month, year):
    if month and year:
        try:
            return datetime(year, month, day if day else 1).isoformat()
        except ValueError:
            return None
    else:
        return None


def _build_raw_date(item):
    return {
        'build_day': item.pop('build_day', None),
        'build_month': item.pop('build_month', None),
        'build_year': item.pop('build_year', None),
        'launch_day': item.pop('launch_day', None),
        'launcy_month': item.pop('launch_month', None),
        'launch_year': item.pop('launch_year', None),
        'order_day': item.pop('order_day', None),
        'order_month': item.pop('order_month', None),
        'order_year': item.pop('order_year', None),
        'keel_laid_day': item.pop('keel_laid_day', None),
        'keel_laid_month': item.pop('keel_laid_month', None),
        'keel_laid_year': item.pop('keel_laid_year', None),
        'dead_day': item.pop('dead_day', None),
        'dead_month': item.pop('dead_month', None),
        'dead_year': item.pop('dead_year', None),
    }


def _detect_compliance(item):
    """
    We decide the value of compliance_method based on three other attributes.
    https://kpler1.atlassian.net/browse/DS-131.

    The compliance column is right now filled using two sources, one is Gibson and other is Imo
    website.
    """

    filtered_items = list(
        k
        for k in ["scrubber_fitted", "scrubber_planned", "scrubber_ready"]
        if (k in item) and item[k]
    )
    if len(filtered_items) > 0:
        return COMPLIANCE_MAPPING[filtered_items[0]]

    return None


def vessel_mapping():
    return {
        'BaleCubic': ('mass_capacity_bale', lambda x: try_apply(x, float, int)),
        'BeamMoulded': ('beam', lambda x: try_apply(x, float, int)),
        'BuiltMonth': ('build_month', lambda x: try_apply(x, int)),
        'BuiltYear': ('build_year', lambda x: try_apply(x, int)),
        'BuiltDay': ('build_day', lambda x: try_apply(x, int)),
        'CallSign': ('call_sign', _clean_string),
        # TODO downstream loaders are not capable of processing charterer info
        # from vessel loader
        'Charterer': ignore_key('current vessel charterer; may be time/spot'),
        'CommercialOwnerEffDate': (
            'owner_date_of_effect',
            lambda x: to_isoformat(x, dayfirst=False),
        ),
        'Draft': ('draught', lambda x: try_apply(x, float)),
        'DWT': ('dead_weight', lambda x: try_apply(x, float, int)),
        # TODO downstream loaders are not capable of processing charterer info
        # from vessel loader
        'EffectiveControlEffDate': ignore_key('charterer date of effect'),
        'EffectiveControlExpiry': ignore_key('charterer date of expiry'),
        'FlagCode': ('flag', lambda x: constants.FLAG_MAPPING.get(x, (None, None))),
        'GrainCubic': ('mass_capacity_grain', lambda x: try_apply(x, float, int)),
        'GT': ('gross_tonnage', lambda x: try_apply(x, float, int)),
        'IMONumber': ('imo', lambda x: x if x and x[0].isdigit() else None),
        'LaunchMonth': ('launch_month', lambda x: try_apply(x, int)),
        'LaunchYear': ('launch_year', lambda x: try_apply(x, int)),
        'LaunchDay': ('launch_day', lambda x: try_apply(x, int)),
        'LiquidCubic98Pcnt': ('volume_capacity', lambda x: try_apply(x, float)),
        'LOA': ('length', lambda x: try_apply(x, float, int)),
        'NT': ('net_tonnage', lambda x: try_apply(x, float, int)),
        'OrderMonth': ('order_month', lambda x: try_apply(x, int)),
        'OrderYear': ('order_year', lambda x: try_apply(x, int)),
        'OrderDay': ('order_day', lambda x: try_apply(x, int)),
        'OreCubic': ('mass_capacity_ore', lambda x: try_apply(x, float)),
        'Owner': ('owner', _clean_player_name),
        'Manager': ('manager', _clean_player_name),
        'PrimaryOperatorEffDate': (
            'manager_date_of_effect',
            lambda x: to_isoformat(x, dayfirst=False),
        ),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
        'SubtypeCode': ('vessel_subtype', _clean_string),
        'SubtypeName': ignore_key('longer vessel subtype name'),
        'TradingCategoryCode': ignore_key('vessel status code'),
        'TradingCategoryName': ignore_key('vessel status'),
        'TradingStatusCode': ('status', _clean_string),
        'TradingStatusName': ignore_key('vessel status'),
        'TypeName': ignore_key('longer vessel type name'),
        'VesselName': ('name', _clean_string),
        'VesselTypeCode': ('vessel_type', _clean_string),
        'KeelLaidYear': ('keel_laid_year', lambda x: try_apply(x, int)),
        'KeelLaidMonth': ('keel_laid_month', lambda x: try_apply(x, int)),
        'KeelLaidDay': ('keel_laid_day', lambda x: try_apply(x, int)),
        'DeadYear': ('dead_year', lambda x: try_apply(x, int)),
        'DeadMonth': ('dead_month', lambda x: try_apply(x, int)),
        'DeadDay': ('dead_day', lambda x: try_apply(x, int)),
        'LastDryDock': ('last_dry_dock', lambda x: _clean_date(x)),
        'LastSpecialSurvey': ('last_special_survey', lambda x: _clean_date(x)),
        # denotes the displacement when the vessel is loaded
        # NOTE This needs to be confiremd with Gibson.
        'Displacement': ('displacement', lambda x: try_apply(x, int)),
        'LBP': ('length_between_perpendiculars', lambda x: try_apply(x, float)),
        'Depth': ('depth', lambda x: try_apply(x, float)),
        'LadenSpeed': ('laden_speed', lambda x: try_apply(x, float)),
        'BallastSpeed': ('ballast_speed', lambda x: try_apply(x, float)),
        'LadenConsumption': ('ladenconsumption', lambda x: try_apply(x, float)),
        'BallastConsumption': ('ballastconsumption', lambda x: try_apply(x, float)),
        'TPCMI': ('tpcmi', lambda x: try_apply(x, float, int)),
        # LDT denotes the displacement when the vessel is in ballast
        'LDT': ('light_displacement', lambda x: try_apply(x, float)),
        'NTSuez': ('net_tonnage_suez', lambda x: try_apply(x, float)),
        'NTPanama': ('net_tonnage_panama', lambda x: try_apply(x, float)),
        'ScrubberFitted': ('scrubber_fitted', lambda x: try_apply(x, int, bool)),
        'ScrubberReady': ('scrubber_ready', lambda x: try_apply(x, int, bool)),
        'ScrubberPlanned': ('scrubber_planned', lambda x: try_apply(x, int, bool)),
        'ScrubberTypeName': ('scrubber_type', None),
        'ScrubberDate': ('scrubber_date', lambda x: _clean_date(x)),
    }


def _build_players(item):
    """Mutate current item to have owner/manager/operator roles under a `companies` field.

    Args:
        item [Dict[str, Any]]:

    """
    item['companies'] = item.get('companies', [])

    owner, owner_date = item.pop('owner', None), item.pop('owner_date_of_effect', None)
    if owner:
        item['companies'].append(
            {'name': owner, 'date_of_effect': owner_date, 'role': PlayerRole.owner}
        )

    manager, manager_date = item.pop('manager', None), item.pop('manager_date_of_effect', None)
    if manager:
        item['companies'].append(
            {'name': manager, 'date_of_effect': manager_date, 'role': PlayerRole.ship_manager}
        )


def _clean_player_name(raw_name):
    """Clean player name.

    FIXME If the raw player name contains `/`,
    we consider the vessel as having shared management/ownership and discard it.
    This is done because the source does not provide the shares of each player, hence we cannot
    insert them and assume an equal split, unless analysts explicitly want it.

    Args:
        raw_name (Optional[str]):

    Returns:
        Optional[str]:

    Examples:
        >>> _clean_player_name(None)
        >>> _clean_player_name('Chandris Group')
        'Chandris Group'
        >>> _clean_player_name('NYK/MBK')
        >>> _clean_player_name('Atlas Shipping A/S')
        'Atlas Shipping A/S'
        >>> _clean_player_name('SK Shipping/H Line')
        >>> _clean_player_name('MOL / China COSCO')
        >>> _clean_player_name('Unknown Chinese')

    """
    # sanity check
    if not raw_name:
        return raw_name

    # handle "unknown" players
    if 'unknown' in raw_name.lower():
        return None

    raw_name = may_strip(raw_name)

    # single company only
    if '/' not in raw_name:
        return raw_name

    # special case; handle company extensions with `/` in them (should not be discarded)
    tokens = raw_name.split()
    for tkn in tokens:
        # 4 is a reasonable length to prevent false positives
        # e.g. A/S, Sp/F, H/F
        if '/' in tkn and tkn != '/' and len(tkn) <= 4:
            return raw_name

    return None


def _clean_string(raw):
    return may_strip(raw) if may_strip(raw) else None


def _clean_date(raw):
    try:
        return to_isoformat(raw, dayfirst=True)
    except (ValueError, TypeError):
        return None
