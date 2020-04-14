from schematics.exceptions import ValidationError
from schematics.types import DateTimeType, ListType, ModelType, StringType

from kp_scrapers.lib.date import ISO8601_FORMAT
from kp_scrapers.models.cargo import Cargo
from kp_scrapers.models.normalize import BaseEvent
from kp_scrapers.models.vessel import Vessel


class PortCall(BaseEvent):
    """Describe a port call schema.

    REQUIRES ALL of the following fields:
        - port_name
        - provider_name (as defined in `BaseEvent`)
        - reported_date
        - vessel

    REQUIRES AT LEAST ONE of the following fields:
        - arrival
        - berthed
        - departure
        - eta

    Optional fields:
        - cargoes
        - installation
        - next_zone
        - berth
        - shipping_agent

    """

    arrival = DateTimeType(
        metadata='arrival timestamp of vessel at specified port of call',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    berthed = DateTimeType(
        metadata='berthed timestamp of vessel at specified port of call',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    berth = StringType(metadata='name of specific berth in an installation')
    cargoes = ListType(
        metadata='list of dicts of cargo movements onto/off the vessel', field=ModelType(Cargo)
    )
    departure = DateTimeType(
        metadata='departure timestamp of vessel at specified port of call',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    eta = DateTimeType(
        metadata='ETA timestamp of vessel at specified port of call',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    installation = StringType(metadata='name of specific installation called at port of call')
    port_name = StringType(metadata='name of port of call', required=True)
    next_zone = StringType(
        metadata='name of region/zone/port where vessel will call at after this port call'
    )
    reported_date = DateTimeType(
        metadata='date on which data is recorded by provider',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
        required=True,
    )
    shipping_agent = StringType(metadata='shipping agent')
    vessel = ModelType(metadata='dict of vessel attributes', model_spec=Vessel, required=True)

    def validate_eta(self, model, eta):
        if self._validate_date(model):
            return eta

    def validate_arrival(self, model, arrival):
        if self._validate_date(model):
            return arrival

    def validate_departure(self, model, departure):
        if self._validate_date(model):
            return departure

    def validate_berthed(self, model, berthed):
        if self._validate_date(model):
            return berthed

    @staticmethod
    def _validate_date(model):
        """Validate on a model-level that there exists at least one eta/arrival/berthed/departure.
        """
        if all(not model.get(x) for x in ('eta', 'arrival', 'berthed', 'departure')):
            raise ValidationError(
                'Port call must have at least one associated ETA/arrival/berthed/departure'
            )

        return True


class CargoMovement(PortCall):
    """Describe a port call schema, specialised as a cargo movement event.

    NOTE This model is identical to PortCall, except that it also allows a `cargo` field.

    FIXME Ideally, we should merge this model and PortCall, except that the ETL does not
    allow for it yet. Therefore, this model remains as a workaround, to distinguish portcall data
    that should load cargo movement data AND create portcalls, from portcall data that should
    purely be loaded as cargo movement data ONLY.

    REQUIRES ALL of the following fields:
        - port_name
        - provider_name (as defined in `BaseEvent`)
        - reported_date
        - vessel

    REQUIRES AT LEAST ONE of the following fields:
        - arrival
        - berthed
        - departure
        - eta

    Optional fields:
        - cargoes OR cargo
        - installation
        - next_zone
        - berth
        - shipping_agent

    """

    cargo = ModelType(metadata='attributes of cargo movement onto/off the vessel', model_spec=Cargo)
