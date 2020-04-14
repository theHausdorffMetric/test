from schematics.types import DateTimeType, FloatType, StringType

from kp_scrapers.lib.date import ISODATE_WITH_SPACE
from kp_scrapers.models.normalize import BaseEvent


class ExchangeRate(BaseEvent):
    """Exchange rate model describes the rate of various currencies with
    respect to USD as the base currency

    REQUIRES ALL of the following fields:
        - date_utc
        - currency_code
        - rate
    """

    date_utc = DateTimeType(
        metadata='date on which data is recorded by provider',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISODATE_WITH_SPACE,
        required=True,
    )
    currency_code = StringType(metadata='official symbol for common currencies', required=True,)
    rate = FloatType(
        metadata='exchange rate with respect to usd as the base currency', required=True,
    )
