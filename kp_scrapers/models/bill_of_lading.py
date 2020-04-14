from schematics.types import DateTimeType, DictType, ModelType, StringType

from kp_scrapers.lib.date import ISO8601_FORMAT
from kp_scrapers.models.cargo import Cargo
from kp_scrapers.models.normalize import BaseEvent
from kp_scrapers.models.vessel import Vessel


class BillOfLading(BaseEvent):
    """Describe a bill of lading schema.

    REQUIRES ALL of the following fields:
        - arrival_date
        - cargo
        - provider_name (as defined in `BaseEvent`)
        - vessel

    Optional fields:
        - bill_of_lading_id
        - carrier
        - consignee
        - destination_port (this should actually be mandatory, but some sources don't provide it,
          but still have good cargo data, so we keep it optional to increase product coverage, at
          the expense of more required human verification)
        - distribution_port
        - ext_voyage_id
        - house_vs_master
        - in_bond_entry_type
        - marks
        - master_bill_of_lading_id
        - notify_party
        - origin_country
        - origin_port
        - place_of_receipt
        - shipper

    """

    arrival_date = DateTimeType(
        metadata='arrival timestamp of vessel at specified discharge port',
        tzd='allow',  # use timezone data if provided, else omit
        convert_tz=True,  # convert tz to UTC, if present
        serialized_format=ISO8601_FORMAT,
    )
    bill_of_lading_id = StringType(metadata='')
    carrier = DictType(field=StringType, metadata='carrier details')
    cargo = ModelType(
        metadata='attributes of cargo as detailed in bill of lading', model_spec=Cargo
    )
    consignee = DictType(field=StringType, metadata='consignee details')
    destination_port = StringType(metadata='name of discharge port')
    distribution_port = StringType(metadata='name of distribution port')
    ext_voyage_id = StringType(metadata='voyage number as stated on bill of lading')
    house_vs_master = StringType(metadata='house vs master')
    in_bond_entry_type = StringType(metadata='in-bond entry type')
    marks = StringType(metadata='marks and numbers')
    master_bill_of_lading_id = StringType(metadata='master bill of lading number')
    notify_party = DictType(field=StringType, metadata='notifying party details')
    origin_country = StringType(metadata='name of loading country')
    origin_port = StringType(metadata='name of loading port')
    place_of_receipt = StringType(metadata='place of receipt of bill of lading')
    shipper = DictType(field=StringType, metadata='shipper details')
    vessel = ModelType(metadata='dict of vessel attributes', model_spec=Vessel, required=True)
