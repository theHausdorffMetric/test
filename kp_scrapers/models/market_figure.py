from schematics.exceptions import ValidationError
from schematics.types import DateTimeType, FloatType, StringType

from kp_scrapers.lib.date import ISO8601_FORMAT
from kp_scrapers.models.enum import Enum
from kp_scrapers.models.normalize import BaseEvent
from kp_scrapers.models.units import Unit


BalanceType = Enum(
    import_='Import',
    export='Export',
    stock_change='Stock change',
    refinery_intake='Refinery intake',
    direct_use='Direct use',
    production='Production',
    ending_stocks='Ending stocks',
)

CountryType = Enum(
    country_checkpoint='country_checkpoint', country='country', region='region', custom='custom'
)


class MarketFigure(BaseEvent):
    """Describe a market figure schema.

    REQUIRES ALL of the following fields:
        - balance
        - country
        - country_type
        - period
        - product
        - reported_date
        - unit

    REQUIRES AT LEAST ONE of the following fields:
        - mass
        - volume

    """

    balance = StringType(
        metadata='Type of market figure', required=True, choices=[value for _, value in BalanceType]
    )
    country = StringType(metadata='Country name', required=True)
    country_type = StringType(
        metadata='Country type', required=True, choices=[value for _, value in CountryType]
    )
    mass = FloatType(metadata='Mass quantity in tons')
    start_date = DateTimeType(
        metadata='starting date of the Market figure time period',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
        required=True,
    )
    end_date = DateTimeType(
        metadata='ending date of the Market figure time period',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
        required=True,
    )
    product = StringType(metadata='Product name', required=True)
    reported_date = DateTimeType(
        metadata='date on which data is recorded by provider',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
        required=True,
    )
    unit = StringType(
        metadata='Quantity unit',
        required=True,
        choices=[Unit.barrel, Unit.tons, Unit.liter, Unit.kiloliter],
    )
    volume = FloatType(metadata='Volume quantity in barrels')

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

        return True
