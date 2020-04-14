from schematics.types import DateTimeType, FloatType, StringType

from kp_scrapers.lib.date import ISO8601_FORMAT
from kp_scrapers.models.enum import Enum
from kp_scrapers.models.normalize import BaseEvent


FuelType = Enum(IFO380='IFO380', MGO='MGO',)


class BunkerFuelCost(BaseEvent):
    """Bunker fuel oil prices

    It is used by the voyage-calculator to estimate the total fuel cost during a
    voyage.  There is two type of fuel, IF0380 and MGO. Due to regulation, vessels
    have to use one or the other, depending on the water they sails.  We use the
    price of "Global 4 Ports Average" as an average of the world bunker price.

    REQUIRES ALL of the following fields:
        - reported_date
        - price
        - type
        - zone
    """

    fuel_type = StringType(
        metadata='Type of fuel', required=True, choices=[value for _, value in FuelType]
    )
    price = FloatType(metadata='price of the fuel', required=True,)
    zone = StringType(metadata='zone involved in pricing of fuel', required=True,)
    reported_date = DateTimeType(
        metadata='date on which data is recorded by provider',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
        required=True,
    )
