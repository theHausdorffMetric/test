from schematics.models import Model
from schematics.types import DateTimeType, FloatType, IntType, ListType, ModelType, StringType

from kp_scrapers.lib.date import ISO8601_FORMAT
from kp_scrapers.models.normalize import BaseEvent


class Coordinates(Model):
    lat = FloatType(metadata='latitude', required=True)
    lon = FloatType(metadata='longitude', required=True)


class ForecastData(Model):
    """This model contains the wind speed that is expected
    to reach on a forecasted date and coordinate
    """

    date = DateTimeType(
        metadata='forecasted date',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    wind = IntType(metadata='forecasted wind speed in knots', required=True)
    position = ModelType(metadata='forecasted coordinates', required=True, model_spec=Coordinates)


class TropicalStorm(BaseEvent):
    """Describe a tropical storm schema.

    REQUIRES ALL of the following fields:
        - name
        - expiration_utc
        - report_utc
        - raw_report_date
        - raw_position
        - raw_forecast
        - forecast data

    OPTIONAL fields:
        - pretty_name
        - description
        - forecast
        - forecast_data
        - winds_sustained
        - winds_gust
        - raw_last_forecast_date

    """

    name = StringType(metadata='Unique ID of cyclone', required=True)
    pretty_name = StringType(metadata='Optional name usable for display', required=True)
    description = StringType(metadata='Additional informations about the report', required=False)
    report_utc = DateTimeType(
        metadata='Date from report recorded by provider',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
        required=True,
    )
    expiration_utc = DateTimeType(
        metadata='Expiration date from report recorded by provider',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
        required=True,
    )
    latitude = FloatType(
        metadata='Average wind speed measured at current position during one minute.', required=True
    )
    longitude = FloatType(
        metadata='Maximum peak wind speed measured at current position during one minute.',
        required=True,
    )
    forecast_data = ListType(
        metadata='More precise meta data for each coordinate',
        field=ModelType(ForecastData),
        required=False,
    )
    forecast = StringType(metadata='Forecasted polygon of impacted area', required=False)
    winds_sustained = IntType(
        metadata='Average wind speed measured at current position during one minute.',
        required=False,
    )
    winds_gust = IntType(
        metadata='Maximum peak wind speed measured at current position during one minute.',
        required=False,
    )
    # TODO: allow the db to record date at which it was scraped
    reported_date = DateTimeType(
        metadata='date on which data is recorded by provider',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
        required=True,
    )
    # TODO: the following items can be removed from the model once changes to the
    # etl/frontend are made
    raw_position = StringType(metadata='Forecasted impacted area, polygon', required=True)
    raw_report_date = StringType(metadata='Forecasted impacted area, polygon', required=True)
    raw_last_forecast_date = StringType(
        metadata='Forecasted impacted area, polygon', required=False
    )
    raw_forecast = StringType(metadata='Forecasted impacted area, polygon', required=False)
