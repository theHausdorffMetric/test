from schematics.exceptions import ValidationError
from schematics.models import Model
from schematics.types import (
    BooleanType,
    DateTimeType,
    DictType,
    FloatType,
    IntType,
    ListType,
    ModelType,
    StringType,
)

from kp_scrapers.lib.date import ISO8601_FORMAT
from kp_scrapers.models.enum import Enum
from kp_scrapers.models.normalize import BaseEvent
from kp_scrapers.models.validators import is_positive_number, is_valid_build_year, is_valid_imo


# TODO it deserves to be elsewhere
PlayerRole = Enum(
    builder='Builder',
    insurer='Insurer',
    ism_manager='ISM Manager',
    operator='Operator',
    owner='Registered owner',
    ship_manager='Ship manager/Commercial manager',
)


VesselStatus = Enum(
    broken_up='Broken Up',
    cancelled_order='Cancelled Order',
    converting='Converting/Rebuilding',
    existence_in_doubt='Continued Existence In Doubt',
    in_casualty='In Casualty Or Repairing',
    in_service='In Service/Commission',
    laid_up='Laid-Up',
    launched='Launched',
    on_order='On Order/Under Construction',
    to_be_broken_up='To Be Broken Up',
    total_loss='Total Loss',
    us_reserve_fleet='U.S. Reserve Fleet',
    unknown='Unknown',
)


# TODO Not sure if this belongs here.
# On one hand, it could help us spot issues early on and
# from the start we know/document the kind of statuses Kpler supports.
# On the other hand, this is now super coupled with the ETL and
# the list may not be exhaustive
VesselType = Enum(
    # gas tankers
    lng_lpg_tanker='Combination Gas Tanker (LNG/LPG)',
    lng_tanker='LNG Tanker',
    lpg_tanker='LPG Tanker',
    lpg_chemical_tanker='LPG/Chemical Tanker',
    # stricly chemical tankers
    chemical_tanker='Chemical Tanker',
    # oil tankers
    oil_crude_tanker='Crude Oil Tanker',
    oil_chemical_tanker='Oil/Chemical Tanker',
    oil_products_tanker='Crude/Oil Products Tanker',
    # oil/bulk combination
    bulk_oil_carrier='Bulk/Oil Carrier',
    # bulk carriers
    bulk_carrier='Bulk Carrier',
    general_cargo='General Cargo Ship',
    heavy_load_carrier='Heavy Load Carrier',
    ore_carrier='Ore Carrier',
    ore_oil_carrier='Ore/Oil Carrier',
    transshipment='Trans Shipment Vessel',
)


class Player(Model):
    """Describe a player schema.

    REQUIRES AT LEAST ONE of the following fields:
        - imo
        - name

    Optional fields:
        - address
        - date_of_effect
        - role

    """

    address = StringType(metadata='address of company offices')
    date_of_effect = DateTimeType(
        metadata='date on which player takes control over an associated entity, if any',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    imo = StringType(metadata='unique company IMO number', validators=[is_valid_imo])
    name = StringType(metadata='name of the player')
    role = StringType(metadata='company classification', choices=[v for _, v in PlayerRole])

    def validate_name(self, model, name):
        if self._validate_player(model):
            return name

    def validate_imo(self, model, imo):
        if self._validate_player(model):
            return imo

    @staticmethod
    def _validate_player(model):
        """Validate on a model-level that there exists at least a player name or IMO number.
        """
        if not (model.get('name') or model.get('imo')):
            raise ValidationError('Player must have a name or valid IMO number')

        return True


class Vessel(Model):
    """Describe a vessel identification schema.

    TODO Ideally, we should combine Vessel with VesselRegistry to minimize repetition.
    However, portcall/charter spiders only need to identify a vessel associated with
    its event and so fewer fields are required, as opposed to registry spiders that
    need to fetch as much properties as they can about a vessel for updating on the ETL.

    In short:
        - Vessel:         identifies a vessel in a PortCall/SpotCharter/etc...
        - VesselRegistry: describes a vessel and its properties exhaustively

    REQUIRES AT LEAST ONE of the following fields:
        - imo
        - name

    Optional fields:
        - beam
        - build_year
        - call_sign
        - dead_weight / dwt
        - flag_code / flag_name
        - gross_tonnage
        - length
        - mmsi
        - status
        - vessel_type / type

    """

    beam = IntType(metadata='width overall of vessel', validators=[is_positive_number])
    build_year = IntType(metadata='build year of vessel', validators=[is_valid_build_year])
    call_sign = StringType(metadata='unique alphanumeric callsign of vessel')
    dead_weight = IntType(metadata='deadweight tonnage of vessel', validators=[is_positive_number])
    # TODO deprecate `dwt` in favour of `dead_weight`
    dwt = IntType(metadata='deadweight tonnage of vessel', validators=[is_positive_number])
    # NOTE should we have a predefined list of flag codes then ?
    flag_code = StringType(
        metadata='ISO 3166-1-alpha-2 code of country under which vessel is registered'
    )
    # TODO deprecate `flag_name` in favour of ISO 3166-1 alpha-2 country codes in `flag_code`
    flag_name = StringType(metadata='name of country under which vessel is registered')
    gross_tonnage = IntType(metadata='gross tonnage of vessel', validators=[is_positive_number])
    # NOTE Some vessels navigating only within European union use a ENI instead
    # of an IMO. It proved to be an issue with our AIS providers who either
    # don't know about it or (most of the time and rightfully) treat it as
    # something different.
    imo = StringType(metadata='unique vessel IMO number', validators=[is_valid_imo])
    length = IntType(metadata='length overall of vessel', validators=[is_positive_number])
    mmsi = StringType(metadata='unique vessel MMSI; may change throughout vessel lifespan')
    name = StringType(metadata='name of vessel (may not be unique)')
    # TODO deprecate `type` in favour of non-built-in `vessel_type`
    type = StringType(metadata='name of vessel')
    vessel_type = StringType(metadata='name of vessel')

    def validate_dwt(self, model, dwt):
        # forward compatibility with new `dead_weight` field to replace `dwt`
        if not model.get('dead_weight'):
            model.update(dead_weight=dwt)
        return dwt

    def validate_name(self, model, name):
        if self._validate_vessel_id(model):
            return name

    def validate_imo(self, model, imo):
        if self._validate_vessel_id(model):
            return imo

    @staticmethod
    def _validate_vessel_id(model):
        """Validate on a model-level that there exists at least a vessel name or IMO number.
        """
        if not (model.get('name') or model.get('imo')):
            raise ValidationError('Vessel must have a name or valid IMO number')

        return True


class VesselRegistry(BaseEvent):
    """Describe a vessel registry schema.

    TODO Ideally, we should combine VesselRegistry with Vessel to minimize repetition,
    however registry spiders trying to fetch as much properties as they can about a
    vessel for updating on the ETL, and so more fields here need to be mandatory.

    In short:
        - Vessel:         identifies a vessel in a PortCall/SpotCharter/etc...
        - VesselRegistry: describes a vessel and its properties exhaustively

    REQUIRES ALL of the following fields:
        - imo

    CONDITIONALLY REQUIRES `vessel_type` if `dead_weight` is supplied

    Optional fields:
        - beam
        - build_year
        - call_sign
        - companies
        - classification_surveys
        - classification_statuses
        - dead_weight
        - flag_code / flag_name
        - gross_tonnage
        - length
        - mmsi
        - name
        - status
        - vessel_type / type

    """

    beam = IntType(metadata='width overall of vessel', validators=[is_positive_number])
    build_year = IntType(metadata='build year of vessel', validators=[is_valid_build_year])
    call_sign = StringType(metadata='unique alphanumeric callsign of vessel')
    companies = ListType(
        metadata='players involved in the management of the vessel', field=ModelType(Player)
    )
    # TODO clarify `classification_surveys` and `classification_statuses` contracts
    classification_surveys = ListType(
        metadata=(
            'list of classification surveys done for certifying'
            'that the vessel complies to latest regulatory standards'
        ),
        field=DictType(StringType),
    )
    classification_statuses = ListType(
        metadata=(
            'list of the status of classification surveys done for certifying'
            'that the vessel complies to latest regulatory standards'
        ),
        field=DictType(StringType),
    )
    dead_weight = IntType(metadata='deadweight tonnage of vessel', validators=[is_positive_number])
    # NOTE should we have a predefined list of flag codes then ?
    flag_code = StringType(
        metadata='ISO 3166-1-alpha-2 code of country under which vessel is registered'
    )
    # TODO deprecate `flag_name` in favour of ISO 3166-1 alpha-2 country codes in `flag_code`
    flag_name = StringType(metadata='name of country under which vessel is registered')
    gross_tonnage = IntType(metadata='gross tonnage of vessel', validators=[is_positive_number])
    # NOTE Some vessels navigating only within European union use a ENI instead
    # of an IMO. It proved to be an issue with our AIS providers who either
    # don't know about it or (most of the time and rightfully) treat it as
    # something different.
    imo = StringType(metadata='unique vessel IMO number', validators=[is_valid_imo], required=True)
    length = IntType(metadata='length overall of vessel', validators=[is_positive_number])
    mmsi = StringType(metadata='unique vessel MMSI; may change throughout vessel lifespan')
    name = StringType(metadata='name of vessel (may not be unique)')
    status = StringType(
        metadata='operational status of vessel', choices=[v for _, v in VesselStatus]
    )
    # TODO deprecate `type` in favour of non-built-in `vessel_type`
    type = StringType(metadata='type of vessel')
    vessel_type = StringType(metadata='type of vessel')

    # fields for tracking additional info about vessel
    build_at = DateTimeType(
        metadata='derived date from build_day, build_month, build_year',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    order_at = DateTimeType(
        metadata='derived date from order_day, order_month, order_year',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    keel_laid_at = DateTimeType(
        metadata='derived date from keel_laid_day, keel_laid_month, keel_laid_year',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    launch_at = DateTimeType(
        metadata='derived date from launch_day, launch_month, launch_year',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    dead_at = DateTimeType(
        metadata='derived date from dead_day, dead_month, dead_year',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    raw_date = DictType(StringType)
    reported_date = DateTimeType(
        metadata='date of retrieved info from gibson',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    last_special_survey = DateTimeType(
        metadata='date of retrieved info from gibson',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    last_dry_dock = DateTimeType(
        metadata='date of retrieved info from gibson',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )

    ballast_speed = FloatType(validators=[is_positive_number])
    ballastconsumption = FloatType(validators=[is_positive_number])
    laden_speed = FloatType(validators=[is_positive_number])
    ladenconsumption = FloatType(validators=[is_positive_number])

    net_tonnage_panama = FloatType(validators=[is_positive_number])
    net_tonnage_suez = FloatType(validators=[is_positive_number])
    net_tonnage = FloatType(validators=[is_positive_number])

    depth = FloatType(validators=[is_positive_number])
    length_between_perpendiculars = FloatType(validators=[is_positive_number])
    draught = FloatType(validators=[is_positive_number])
    light_displacement = FloatType(
        metadata='displacement when the vessel is in ballast', validators=[is_positive_number]
    )

    displacement = IntType(
        metadata='water displaced when the vessel is loaded', validators=[is_positive_number]
    )

    volume_capacity = FloatType(validators=[is_positive_number])
    mass_capacity_bale = FloatType(validators=[is_positive_number])
    mass_capacity_grain = FloatType(validators=[is_positive_number])
    mass_capacity_ore = FloatType(validators=[is_positive_number])

    tpcmi = FloatType(validators=[is_positive_number])
    scrubber_date = DateTimeType(
        metadata='date of scrubber installation',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    scrubber_type = StringType(metadata='Type of scrubber attached')
    scrubber_planned = BooleanType()
    scrubber_ready = BooleanType()
    scrubber_fitted = BooleanType()
    compliance_method = StringType(metadata='type of compliance used in the vessel')

    def validate_type(self, model, _type):
        if self._validate_vessel_type(model):
            return _type

    def validate_vessel_type(self, model, vessel_type):
        if self._validate_vessel_type(model):
            return vessel_type

    @staticmethod
    def _validate_vessel_type(model):
        """Validate on a model-level that there exists a vessel type.
        """
        if model.get('dead_weight') and not (model.get('type') or model.get('vessel_type')):
            raise ValidationError('Vessel must have a type if deadweight is supplied')

        # forward compatibility with new `vessel_type` field to replace `type`
        if not model.get('vessel_type'):
            model.update(vessel_type=model.get('type'))
        else:
            model.update(type=model['vessel_type'])
        return True
