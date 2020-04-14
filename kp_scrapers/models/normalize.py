import uuid

from schematics.models import Model
from schematics.types import StringType, UUIDType
import semver

from kp_scrapers import __version__ as _package_version
from kp_scrapers.models.enum import Enum


DataTypes = Enum(
    Ais='ais',
    BillOfLading='bill_of_lading',
    BunkerFuelCost='bunker_fuel_cost',
    Cargo='cargo',
    CargoMovement='cargo_movement',
    CustomsFigure='customs_figure',
    CustomsPortCall='customs_portcall',
    ExchangeRate='exchange_rate',
    MarketFigure='market_figure',
    PortCall='portcall',
    SpotCharter='spotcharter',
    StockExchange='stock_exchange',
    Vessel='vessel',
    TropicalStorm='tropical_storm',
)


class BaseEvent(Model):
    """Initialise meta fields for an event.

    NOTE this model should ideally also document the presence of `scrapy-magicfields`,
    since they are required by the ETL:
        - sh_item_time   : str('%Y-%m-%dT%H:%M:%S.%f')
        - sh_job_id      : str($env:SCRAPY_JOB)
        - sh_job_time    : str('%Y-%m-%d %H:%M:%S')
        - sh_spider_name : str()

    The issue is, when `@normalize_item(BaseEvent, normalize=True)` is decorated on a spider,
    magicfields won't appear at all if they are defined in this Model, locally, or on scrapinghub.
    Removing them from this Model solves the issue, and is theoretically neater since Models
    should not care about additional fields added by middlewares. For practical reasons as stated
    above, they should be here, but they are not.

    REQUIRES ALL of the following fields:
        - provider_name

    """

    # will be filled in by `kp_scrapers.pipelines.context.EnrichItemContext` extension
    kp_package_version = StringType(default=_package_version, validators=[semver.parse])
    kp_source_version = StringType(validators=[semver.parse])
    kp_uuid = UUIDType(default=uuid.uuid4)

    # ETL expects `_type` field when checking event type
    _type = StringType(metadata='name of model, useful for categorising serialised items')

    provider_name = StringType(metadata='name of data provider', required=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._type = self.__class__.__name__
