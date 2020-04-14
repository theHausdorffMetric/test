# -*- coding: utf-8 -*-

"""Scrapy items models.

See documentation in: http://doc.scrapy.org/en/latest/topics/items.html

"""

from __future__ import absolute_import, unicode_literals
from datetime import datetime

from scrapy.item import Field, Item

from kp_scrapers.models.base import VersionedItem
from kp_scrapers.models.utils import filter_item_fields


def as_item(item_type):
    """
    Decorator that makes a new `item_type` object from a function's output dict.
    The dict is filtered to only keep the items expected by `item_type`.
    Args:
        item_type (scrapy.item.Item)
    Returns:
        callable
    """

    def _outer_wrapper(fn):
        def _wrapper(*args, **kwargs):
            item = fn(*args, **kwargs)
            filtered_item = filter_item_fields(item_type, item)
            return item_type(**filtered_item)

        return _wrapper

    return _outer_wrapper


class VesselPortCall(VersionedItem, Item):
    arrival_date = Field()
    arrival_draught = Field()
    berth = Field()  # str
    berthing_time = Field()
    call_sign = Field()
    # When the destination indicated is not the port but some location beyond
    # the port.
    cargo_destination = Field()
    cargo_mmbtu = Field()  # float
    # Precision about the action performed on the cargo:
    #   loading, unloading, ...
    cargo_movement = Field()
    cargo_operator = Field()
    cargo_ton = Field()
    cargo_type = Field()
    cargo_volume = Field()
    # Retrieved from the vessel list containing all the vessels of all the commodities
    # To facilitate item loading when multi-commo
    commo = Field()
    dead_weight = Field()
    departure_date = Field()
    departure_destination = Field()
    departure_destination_eta = Field()  # str (the da
    departure_draught = Field()
    eta = Field()  # str (the date in UTC, ISO formatted)
    etd = Field()  # str (the date in UTC, ISO formatted)
    foreign = Field()
    from_port_departure_date = Field()
    from_port_name = Field()
    gross_tonnage = Field()
    imo = Field()  # str
    installation = Field()
    length = Field()
    local_port_call_id = Field()
    missing_eta = Field()
    net_tonnage = Field()
    operation_end = Field()
    operation_start = Field()
    origin_eta = Field()  # str (the date in UTC, ISO formatted)
    origin_etd = Field()  # str (the date in UTC, ISO formatted)
    port_name = Field()
    position_in_port = Field()
    provider_voyage_id = Field()
    receiver = Field()
    ship_draught = Field()
    shipping_agent = Field()
    supplier = Field()
    updated_time = Field()
    url = Field()  # str
    vessel_flag = Field()
    vessel_movement = Field()
    vessel_name = Field()
    vessel_status = Field()
    vessel_type = Field()


class PortCall(VersionedItem, Item):
    """Define a PA event that is retro-compatible with CargoMovement loader.

    This exists as a legacy item with `cargo` that is directly compatible with CargoMovement loader,
    since the loader does not support the newer `cargoes` field.

    """

    berth = Field()
    # backward compatibility with cargo loader
    cargo = Field()
    installation = Field()
    matching_date = Field()
    next_port_name = Field()
    port_name = Field()
    previous_port_name = Field()
    receiver = Field()
    reported_date = Field()
    supplier = Field()
    updated_time = Field()
    vessel = Field()
    url = Field()


class Vessel(VersionedItem, Item):
    name = Field()  # required
    imo = Field()
    mmsi = Field()

    breadth = Field()
    build_year = Field()
    builder = Field()
    call_sign = Field()
    capacity = Field()
    charterer = Field()
    classification_status = Field()  # contains classification_society
    classification_surveys = Field()  # contains classification_society
    classifications_history = Field()
    companies = Field()
    companies_history = Field()
    dead_weight = Field()
    draught = Field()
    flag_code = Field()
    flag_name = Field()
    flags_history = Field()
    former_names = Field()
    gross_tonnage = Field()
    home_port = Field()
    insurers = Field()
    last_flag = Field()  # redundant with flags_history
    length = Field()
    manager = Field()  # redundant with names_history
    max_speed = Field()
    names_history = Field()
    net_tonnage = Field()
    owner = Field()
    received_time = Field()
    safety_certificates = Field()
    ship_class = Field()  # redundant with classification_status?
    status = Field()
    status_date = Field()
    type = Field()
    updated_time = Field()
    url = Field()


# only used by `marinetraffic_web` => deprecated it
class VesselPosition(VersionedItem, Item):
    eta = Field()
    eta_updated_time = Field()
    imo = Field()
    mmsi = Field()
    air_temperature = Field()
    ais_source = Field()
    ais_source_type = Field()
    area = Field()
    call_sign = Field()
    category = Field()
    course = Field()
    currently_in_port = Field()
    destination = Field()
    draught = Field()
    heading = Field()
    in_range = Field()
    last_port = Field()
    last_port_start = Field()
    latitude = Field()
    longitude = Field()
    name = Field()
    navigational_status = Field()
    position_type = Field()
    received_time = Field()
    speed = Field()
    url = Field()
    wind_bearing = Field()
    wind_speed = Field()
    miles_run = Field()
    barometer = Field()
    visibility = Field()
    wave_height = Field()
    dew_point = Field()
    water_temperature = Field()


class Customs(VersionedItem, Item):
    url = Field()
    type = Field()
    commodity = Field()
    grade = Field()
    country_name = Field()
    country_code = Field()
    # The non european source country (in two cases : imports and exports)
    source_country = Field()
    terminal = Field()
    port_code = Field()

    raw_price = Field()
    raw_price_currency = Field()
    raw_weight = Field()
    raw_weight_units = Field()
    raw_volume = Field()
    raw_volume_units = Field()
    raw_price_per_mmbtu = Field()

    year = Field()
    month = Field()


class USCustoms(VersionedItem, Item):
    # common
    agent_name = Field()
    dock_name = Field()
    draft = Field()
    filing_date = Field()
    indicated_transaction = Field()
    manifest = Field()
    manifest_end_date = Field()
    manifest_start_date = Field()
    official = Field()
    operator = Field()
    owner = Field()
    port_code = Field()
    port_name = Field()
    total_crew = Field()
    trade_code = Field()
    vessel = Field()
    # clearance
    next_domestic_port = Field()
    next_foreign_country = Field()
    next_foreign_port = Field()
    # entrance
    pax = Field()
    voyage = Field()
    last_domestic_port = Field()
    last_foreign_country = Field()
    last_foreign_port = Field()
    # source
    source = Field()


class VesselPositionAndETA(VersionedItem, Item):
    """Notes on types since Postgres won't cast them for you:

    - Except when specified, fields are expected to be string.
    - Date format: YYYY-MM-DDThh:mm:ss (Python flavored ISO8602)

    Unicity is computed with an md5 on everything but scrapinghub metadatas.

    """

    # str('T-AIS' | 'S-AIS') - message source
    # if not provided, the ETL will try to infere it from position and
    # nextDestination ais_type
    aisType = Field()
    # int - AIS message type
    message_type = Field()
    # NOTE merge this with `provider_name`
    # since it is used to match the table Provider.shortname
    provider_id = Field()

    # NOTE the loader tries to read `timeUpdated`
    # and always fallback on `sh_job_time`

    master_imo = Field()  # str
    master_mmsi = Field()  # str
    master_flag = Field()  # str
    master_callsign = Field()  # str
    master_name = Field()  # str
    # dimensions of the ship where the center is the AIS emitter
    master_dimA = Field()  # int
    master_dimB = Field()  # int
    master_dimC = Field()  # int
    master_dimD = Field()  # int
    # NOTE should be int as it is the id used by AIS (e.g. 80)
    master_shipType = Field()  # str
    # NOTE I think it's not used, or amalgamed with `timeUpdated` (see above)
    master_timeUpdated = Field()  # str(UTC ISO formatted)

    position_navState = Field()
    position_course = Field()  # Union[int,float]
    position_draught = Field()  # Union[int,float]
    position_heading = Field()  # Union[int,float]
    position_lat = Field()  # float
    position_lon = Field()  # float
    position_speed = Field()  # float
    position_aisType = Field()  # Enum['S-AIS','S & T-AIS', 'T-AIS']
    position_timeReceived = Field()  # str (UTC ISO formatted)

    # str (UTC ISO formatted)
    # if this field is not set, all `nextDestination_*` will be ignored
    nextDestination_eta = Field()
    nextDestination_aisType = Field()  # Enum['S-AIS','S & T-AIS', 'T-AIS']
    nextDestination_destination = Field()
    # default to `sh_job_time`
    nextDestination_timeUpdated = Field()

    # !!! TEMPORARY - workaround for exact earth issue !!!
    raw_position_draught = Field()


class Slot(VersionedItem, Item):
    installation_id = Field()
    seller = Field()
    date = Field()
    on_offer = Field()


class RealTimeSendOut(VersionedItem, Item):
    installation = Field()
    date = Field()
    value = Field()


class StockExchangeIndex(VersionedItem, Item):
    raw_unit = Field()
    raw_value = Field()
    converted_value = Field()
    difference_value = Field()
    index = Field()
    ticker = Field()
    zone = Field()
    commodity = Field()
    provider = Field()
    month = Field()
    day = Field()
    source_power = Field()


class USAModuleSendIn(VersionedItem, Item):
    installation_id = Field()
    date = Field()
    value = Field()
    pipeline = Field()
    url = Field()
    unit = Field()


class CycloneWarning(VersionedItem, Item):
    # fields required
    name = Field()
    # ETL is expecting '%y%m%d/%H%M UTC' or None
    raw_report_date = Field()
    # actually used to define cyclone lat and lon values
    raw_position = Field()

    # human clues
    pretty_name = Field()
    description = Field()

    forecast_data = Field()

    # ETL is expecting '%d/%H%M UTC' or None, but without a value it
    # will use one month as default to compute the expiration date.
    raw_last_forecast_date = Field()
    raw_forecast = Field()

    # float value will be match on the ETL with a regex, if not None
    winds_sustained = Field()
    winds_gust = Field()


# Port Authorities Events
class VesselIdentification(VersionedItem, Item):
    imo = Field()
    mmsi = Field()
    call_sign = Field()
    name = Field()
    dwt = Field()  # Dead weight tonnage
    gt = Field()  # Gross tonnage
    nt = Field()  # Net tonnage
    length = Field()
    type = Field()
    flag = Field()
    build_year = Field()


class Cargo(VersionedItem, Item):
    # raw information in case we loose it when deducing stuff
    raw_type = Field()

    # supported values are currently: 'lng', 'lpg', 'oil', 'coal', 'cpp'
    # cargo information but for creating port call events
    commodity = Field()

    # `oil` or `other` (i.e., cpp) string (required only if commodity is `oil`)
    cargo_status = Field()

    # raw product name that will be mapped to something Kpler understands on the ETL - required
    product = Field()

    # either `load` or `discharge` - optional
    movement = Field()
    # cargo quantity - optional
    volume = Field()
    # quantity unit string (optional), usually `tons`
    # cf. https://github.com/Kpler/ct-pipeline/blob/e38b21056bac08ee637df7924912384b39d5fe13/etl/extraction/cargo_movement/process_item.py#L221  # noqa
    # for additional supported units)
    volume_unit = Field()

    ton = Field()
    origin = Field()
    destination = Field()
    operator = Field()
    grade = Field()
    grade_unit = Field()


class Draught(VersionedItem, Item):
    arrival = Field()
    departure = Field()


class PAEventBase(VersionedItem, Item):
    """Common structure for Port Authority Events."""

    # as defined by `Vessel(Identification)` - required
    # `vessel.name` is required as well, `vessel.mmsi` and `imo` being really
    # nice to have to reduce guess work later on
    vessel = Field()

    cargo = Field()  # as defined by `Cargo` - required
    # TODO: refactor all spider/loader to use `cargoes` which is a `List[cargo]`
    #       `cargoes` is now supported indirectly by PA and Cargo loader
    # NOTE: recommended to use `cargoes` since vessels can carry multiple products
    cargoes = Field()

    # ALL time fields must be declared as an ISO-8601 timestamp (recommended to have UTC+0 timezone)
    # e.g. 2018-03-12T15:53:37+00:00 or 2018-03-12T15:53:37
    reported_date = Field()  # time when source is refreshed, defaults to `utcnow()` in __init__
    matching_date = Field()  # identical to `eta/etd/arrived/berthed` for cargo/PA compatibility

    # name to match port to - required
    port_name = Field()

    # fill in if provided by source - optional
    draught = Field()
    berth = Field()
    installation = Field()
    terminal = Field()
    shipping_agent = Field()
    pa_voyage_id = Field()
    pa_port_call_id = Field()
    updated_time = Field()  # deprecated; please use `reported_date`

    # should be an opt-in (auto) debugging tool. Given how lengthy it can be,
    # the field can severely impact item weight for little gain 99% of the time
    # (although being very useful the remaining 1ù of the time to trace back
    # data to its origin)
    url = Field()

    # identify event type, i.e. `berth`, `eta`, `departure`, ...
    _type = Field()

    def __init__(self, *args, **kwargs):
        if self.__class__.__name__ == 'PAEventBase':
            raise TypeError('PAEventBase must be derived to be instantiable')

        super(PAEventBase, self).__init__(*args, **kwargs)
        self['_type'] = str(self.__class__.__name__)
        # fallback to scraping time if not present, may be overriden
        if 'reported_date' not in kwargs and 'reported_date' not in self:
            self['reported_date'] = (
                datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            )


class EtaEvent(PAEventBase):
    eta = Field()
    next_zone = Field()


class EtdEvent(PAEventBase):
    etd = Field()
    next_zone = Field()


class ArrivedEvent(PAEventBase):
    arrival = Field()
    previous_zone = Field()


class DepartedEvent(PAEventBase):
    departure = Field()
    previous_zone = Field()


class BerthedEvent(PAEventBase):
    berthed = Field()


class InPortEvent(PAEventBase):
    port_area = Field()
    move_date = Field()


class CargoMovedEvent(PAEventBase):
    start_date = Field()
    end_date = Field()


class TimeCharter(VersionedItem, Item):
    start_date = Field()
    end_date = Field()
    vessel = Field()
    reported_date = Field()
    charterer = Field()
    owner = Field()
    rate_value = Field()
    rate_raw_value = Field()


class SpotCharter(VersionedItem, Item):
    """Describe a spot charter schema.

    Charters REQUIRES ALL of the following fields:
        - departure_zone (i.e. loading port)
        - lay_can_end
        - lay_can_start
        - source

    Charters REQUIRES AT LEAST ONE of the following fields:
        - imo
        - vessel_name

    Charters may also take these fields optionally:
        - arrival_zone (i.e, unloading port)
        - build_year
        - cargo
        - charterer
        - dwt
        - imo
        - rate_value
        - rate_raw_value
        - reported_date

    """

    departure_zone = Field()
    arrival_zone = Field()
    vessel = Field()
    lay_can_start = Field()
    lay_can_end = Field()
    reported_date = Field()
    charterer = Field()
    seller = Field()
    charter = Field()
    open_date = Field()
    coming_from = Field()
    fixture_id = Field()
    broker_address_commission_max = Field()
    last_done_rate_value = Field()
    last_done_rate_mts = Field()
    rate_mts = Field()
    rate_value = Field()
    rate_raw_value = Field()
    actual_tce_per_day_including_idle_days = Field()
    breakeven_tce = Field()
    ten_percent_return_tce = Field()
    status = Field()
    voyage_raw_text = Field()
    voyage_raw_text2 = Field()
    cargo = Field()


class Flow(VersionedItem, Item):
    commodity = Field()
    release_date = Field()
    volume = Field()
    volume_unit = Field()
    comment = Field()
    import_zone = Field()
    export_zone = Field()
    type = Field()
    # TODO should be `provider_name` like the others. take the opportunity of
    # renaming to kp_data_provider to fix that
    provider = Field()


class BillOfLading(VersionedItem, Item):
    product_description = Field()
    consignee = Field()
    shipper = Field()
    vessel_name = Field()
    arrival_date = Field()
    weight_kg = Field()
    weight_lb = Field()
    foreign_port = Field()
    country_of_origin = Field()
    us_port = Field()
    distribution_port = Field()
    marks = Field()
    consignee_address = Field()
    consignee_zip = Field()
    shipper_address = Field()
    container_count = Field()
    container_id = Field()
    container_type = Field()
    container_quantity = Field()
    container_quantity_unit = Field()
    container_measurements = Field()
    container_measurements_unit = Field()
    bill_of_lading_id = Field()
    house_vs_master = Field()
    master_bill_of_lading_id = Field()
    ext_voyage_id = Field()
    seal = Field()
    ship_country_registration = Field()
    in_bond_entry_type = Field()
    place_of_receipt = Field()
    carrier_name = Field()
    carrier_code = Field()
    carrier_city = Field()
    carrier_state = Field()
    carrier_zip = Field()
    carrier_address = Field()
    notify_party = Field()
    notify_address = Field()


class BunkerCost(VersionedItem, Item):
    price = Field()  # unit: $/mt
    type = Field()  # Enum['IFO380', 'MGO']
    zone = Field()  # zone displayed on website
    date = Field()
