import datetime as dt

from schematics.exceptions import ValidationError
from schematics.types import DateTimeType, ListType, ModelType, StringType

from kp_scrapers.lib.date import ISO8601_FORMAT
from kp_scrapers.models.cargo import Cargo
from kp_scrapers.models.enum import Enum
from kp_scrapers.models.normalize import BaseEvent
from kp_scrapers.models.vessel import Vessel


# synced with `etl.orm.db_scheme.charter.(Raw)SpotCharterStatus`
SpotCharterStatus = Enum(
    on_subs='On Subs',  # charter is under negotiation
    fully_fixed='Fully Fixed',  # charter has been agreed upon on both parties
    in_progress='In Progress',  # charter has started
    finished='Finished',  # charter has been executed and is no longer in effect
    cancelled='Cancelled',  # charter has been cancelled
    failed='Failed',  # charter was never agreed by both players
    replaced='Replaced',  # charter will happen but for another vessel
    updated='Updated',  # charter is the same but some data changed
)


class SpotCharter(BaseEvent):
    """Describe a spot charter schema.

    REQUIRES ALL of the following fields:
        - charterer
        - departure_zone
        - lay_can_start
        - provider_name (as defined in `BaseEvent`)
        - reported_date
        - vessel

    Optional fields:
        - arrival_zone
        - cargo
        - lay_can_end
        - rate_value
        - rate_raw_value
        - seller
        - status

    For more details, see:
    https://drive.google.com/file/d/0Bzn_Qd0IVloFVm4xSTVGU2JVMlE

    """

    arrival_zone = ListType(field=StringType, metadata='list of names of unloading ports')
    cargo = ModelType(metadata='dict of cargo attributes onboard', model_spec=Cargo)
    charterer = StringType(metadata='name of charterer', required=False)
    departure_zone = StringType(metadata='name of loading port', required=True)
    lay_can_end = DateTimeType(
        metadata='timestamp during which vessel is at departure zone (upper bound)',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    lay_can_start = DateTimeType(
        metadata='timestamp during which vessel is at departure zone (lower bound)',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
        required=True,
    )
    # TODO clarify `rate_raw_value` contract between ETL
    rate_raw_value = StringType(metadata='raw value of current charter, per-hour')
    # TODO clarify `rate_value` contract between ETL
    rate_value = StringType(metadata='value of current charter, per-hour')
    reported_date = DateTimeType(
        metadata='date on which data is recorded by provider',
        # FIXME charters loader assumes `dayfirst=True` when parsing, so we can't use ISO-8601
        parser=lambda x: dt.datetime.strptime(x, '%d %b %Y'),
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format='%d %b %Y',
        required=True,
    )
    seller = StringType(metadata='name of seller')
    status = StringType(
        metadata='reported status of spot charter',
        choices=[value for _, value in SpotCharterStatus],
    )
    vessel = ModelType(
        metadata='dict of chartered vessel attributes', model_spec=Vessel, required=True
    )

    def validate_lay_can_end(self, model, lay_can_end):
        """Validate on a model-level if laycan dates are valid.
        """
        if lay_can_end:
            if lay_can_end < model['lay_can_start']:
                raise ValidationError('Laycan end cannot be before laycan start')

        return lay_can_end
