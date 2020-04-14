from schematics.models import Model
from schematics.types import DateTimeType, FloatType, IntType, ModelType, StringType

from kp_scrapers.lib.date import ISO8601_FORMAT
from kp_scrapers.models.enum import Enum
from kp_scrapers.models.normalize import BaseEvent
from kp_scrapers.models.vessel import Vessel


AisType = Enum(dynamic='D-AIS', satellite='S-AIS', terrestrial='T-AIS')


class Position(Model):
    """Describe an position model for AIS message.

    REQUIRES ALL of the following fields:
        - ais_type
        - course
        - lat
        - lon
        - received_time
        - speed

    Optional fields:
        - draught
        - draught_raw
        - heading
        - nav_state

    """

    ais_type = StringType(
        metadata='ais type, source from which ais signal is received',
        required=True,
        choices=[v for _, v in AisType],
    )
    course = FloatType(
        metadata='the course (in degrees) that the subject vessel is reporting according to '
        'AIS transmissions',
        required=True,
    )
    draught = FloatType(metadata='current draft of the vessel')
    # FIXME it's a workaround for exact earth not being reliable with draught
    draught_raw = FloatType(metadata='raw draught value')
    heading = FloatType(
        metadata='the heading (in degrees) that the subject vessel is reporting according '
        'to AIS transmissions'
    )
    lat = FloatType(metadata='latitude', required=True)
    lon = FloatType(metadata='longitude', required=True)
    # ref: https://help.marinetraffic.com/hc/en-us/articles/203990998-What-is-the-significance-of-the-AIS-Navigational-Status-Values-  # noqa
    nav_state = IntType(
        metadata='the AIS Navigational Status of the subject vessel as input by the vessel\'s '
        'crew - more. There might be discrepancies with the vessel\'s detail page when '
        'vessel speed is near zero (0) knots.'
    )
    received_time = DateTimeType(
        metadata='time when the position get updated in UTC',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
        required=True,
    )
    speed = FloatType(
        metadata='the speed that the subject vessel is reporting according to AIS transmissions',
        required=True,
    )


class AisMessage(BaseEvent):
    """Describe an ais signal schema.

    NOTE:
        next_destination_xxx fields ideally should be required, however, the ETL is capable of
        processing ETA of AIS items without next_destination, so we don't want to reduce
        data quality here for now until the ETL code is simplified or refactored.

    REQUIRES ALL of the following fields:
        - ais_type
        - position
        - provider_name
        - reported_date
        - vessel

    Optional fields:
        - message_type
        - next_destination_eta
        - next_destination_ais_type
        - next_destination_destination

    """

    # FIXME duplicated in Position model
    ais_type = StringType(
        metadata='ais type, source from which ais signal is received',
        choices=[v for _, v in AisType],
        required=True,
    )
    # ref: https://www.navcen.uscg.gov/?pageName=AISMessages
    message_type = StringType(metadata='AIS message type, from 1 to 27')

    # if this field is not set, all `nextDestination_*` will be ignored
    next_destination_eta = DateTimeType(
        metadata='next destination eta',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    next_destination_ais_type = StringType(metadata='ais type', choices=[v for _, v in AisType])
    next_destination_destination = StringType(metadata='next destination')

    position = ModelType(metadata='position description', model_spec=Position, required=True)
    provider_name = StringType(metadata='provider short name')
    reported_date = DateTimeType(
        metadata='reported date of the ais message',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    vessel = ModelType(
        metadata='vessel information for current ais message', model_spec=Vessel, required=True
    )
