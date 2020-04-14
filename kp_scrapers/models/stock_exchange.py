import datetime as dt

from schematics.types import DateTimeType, IntType, StringType

from kp_scrapers.models.enum import Enum
from kp_scrapers.models.normalize import BaseEvent
from kp_scrapers.models.validators import is_valid_rate


CommodityType = Enum(lng='lng', lpg='lpg', oil='oil', butane='butane', propane='propane', cpp='cpp')


class StockExchange(BaseEvent):
    """StockExchange model describes the predicted price for a certain commodity in a specific
    zone.

    REQUIRES ALL of the following fields:
        - commodity
        - index
        - month
        - provider_name (as defined in `BaseEvent`)
        - raw_unit
        - raw_value
        - reported_date
        - ticker
        - zone

    Optional fields:
        - converted_value
        - difference_value
        - source_power

    """

    commodity = StringType(
        metadata='commodity referred by the index. It denotes generic cargo product name, on '
        'ETL the loader would forward `StockExchange` to different platform by '
        'commodity name. E.g. `butane`, `propane` and `lpg` would goes to `lpg` platform',
        choices=[value for _, value in CommodityType],
        required=True,
    )

    index = StringType(
        metadata='index name, it has meaning for a defined set of zones (mostly countries)',
        required=True,
    )

    month = DateTimeType(
        metadata='month being referred to on this index. Usually in the form'
        '`Feb16` or `Mar16`, this has been transformed as a date to be'
        'easily ordered and compared.',
        parser=lambda x: dt.datetime.strptime(x, '%Y-%m-01'),
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format='%Y-%m-01',
        required=True,
    )

    raw_unit = StringType(
        metadata='indicates the unit of `raw_rate`, in format of <currency>/<metric>', required=True
    )

    raw_rate = StringType(
        metadata='price extracted from the source with minimal conversions',
        required=True,
        validators=[is_valid_rate],
    )

    reported_date = DateTimeType(
        metadata='report received date',
        parser=lambda x: dt.datetime.strptime(x, '%Y-%m-%d'),
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format='%Y-%m-%d',
        required=True,
    )

    ticker = StringType(
        metadata='ticker is a way to identify which is the corresponding month for a currency.'
        'For example, the front month value for Brent should have a ticker like Brent 1.'
        'The second month Brent value will be like Brent 2',
        required=True,
    )

    zone = StringType(
        metadata='zone associated to the index, most of the time it\'t country name.', required=True
    )

    converted_value = StringType(
        metadata='converted rate value in source', validators=[is_valid_rate]
    )

    difference_value = StringType(
        metadata='difference rate value in source', validators=[is_valid_rate]
    )

    source_power = IntType(
        metadata='source strength. A bigger number means a stronger accuracy and reliability',
        choices=[1, 10, 200, 1000],
        default=1,
    )
