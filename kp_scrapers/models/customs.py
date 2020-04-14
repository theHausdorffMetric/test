from schematics.exceptions import ValidationError
from schematics.models import Model
from schematics.types import DateTimeType, FloatType, ModelType, StringType

from kp_scrapers.lib.date import ISO8601_FORMAT
from kp_scrapers.models.normalize import BaseEvent
from kp_scrapers.models.units import Currency, Unit
from kp_scrapers.models.vessel import Vessel


class Valuation(Model):
    value = FloatType(metadata='quantity of money', required=True)
    currency = StringType(
        metadata='currency of money', required=True, choices=[value for _, value in Currency]
    )


class CustomsFigure(BaseEvent):
    """Describe a customs import/export schema.

    REQUIRES ALL of the following fields:
        - end_utc
        - export_zone
        - import_zone
        - product
        - provider_name (as defined in `BaseEvent`)
        - reported_date
        - start_utc
        - unit
        - valuation

    REQUIRES AT LEAST ONE of the following fields:
        - mass AND mass_unit
        - volume AND volume_unit

    Optional fields:
        - everything else ...

    """

    end_utc = DateTimeType(
        metadata='Closing date of period described by customs',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
        required=True,
    )
    export_zone = StringType(metadata='Country of export', required=True)
    import_zone = StringType(metadata='Country of import', required=True)
    mass = FloatType(metadata='Mass quantity')
    mass_unit = StringType(metadata='Unit of mass quantity', choices=[value for _, value in Unit])
    product = StringType(metadata='Goods imported/exported, according to customs', required=True)
    reported_date = DateTimeType(
        metadata='Date on which data is recorded by provider',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
        required=True,
    )
    start_utc = DateTimeType(
        metadata='Commencement date of period described by customs',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
        required=True,
    )
    valuation = ModelType(
        metadata='Valuation of goods imported/exported, according to customs',
        required=True,
        model_spec=Valuation,
    )
    volume = FloatType(metadata='Volume quantity')
    volume_unit = StringType(
        metadata='Unit of volume quantity', choices=[value for _, value in Unit]
    )

    def validate_mass(self, model, mass):
        if self._validate_quantity(model):
            return mass

    def validate_volume(self, model, volume):
        if self._validate_quantity(model):
            return volume

    @staticmethod
    def _validate_quantity(model):
        """Validate on a model-level that there exists either volume or mass.
        """
        if all(model.get(x) is None for x in ('volume', 'mass')):
            raise ValidationError('Market figure must have either volume or mass')

        if model.get('mass') and not model.get('mass_unit'):
            raise ValidationError('Mass quantity must have an associated unit')

        if model.get('volume') and not model.get('volume_unit'):
            raise ValidationError('Volume quantity must have an associated unit')

        return True


class CustomsPortCall(BaseEvent):
    """Describe a customs port call schema.

    This model describes a vessel clearance/entrance as filed by the receiving/departing ports
    respectively, with data on its next/previous destinations.

    NOTE as of 22 July 2019, this schema only supports data describing US customs data

    REQUIRES ALL of the following fields:
        - dock_name
        - filing_date
        - port_name
        - provider_name (as defined in `BaseEvent`)
        - source
        - purpose
        - vessel

    Optional fields:
        - everything else ...

    """

    manifest = StringType(
        metadata='Unique identifier of clearance/entrance declaration as filed with customs'
    )
    shipping_agent = StringType(metadata='Shipping agent')
    dock_name = StringType(metadata='Name of docking berth/installation of entering/cleared vessel')
    draught = FloatType(metadata='Draught at time of docking')
    last_domestic_port = StringType(metadata='Last domestic (US) port called at by vessel')
    last_foreign_country = StringType(metadata='Last foreign country visited by vessel')
    last_foreign_port = StringType(metadata='Last foreign port called at by vessel')
    next_domestic_port = StringType(metadata='Next domestic (US) port to be called at by vessel')
    next_foreign_country = StringType(metadata='Next foreign country to be visited by vessel')
    next_foreign_port = StringType(metadata='Next foreign port to be scalled at by vessel')
    port_name = StringType(metadata='Port of call as filed with customs authority', required=True)
    purpose = StringType(
        metadata='Purpose of vessel entrance or clearance',
        choices=['load', 'discharge', 'nothing'],
        required=True,
    )
    source = StringType(
        metadata='Type of vessel movement (clearance = departure, entrance = arrival)',
        choices=['clearance', 'entrance'],
        required=True,
    )
    filing_date = DateTimeType(
        metadata='Date on which portcall declaration is filed with customs authority',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
        required=True,
    )
    vessel = ModelType(metadata='dict of vessel attributes', model_spec=Vessel, required=True)
