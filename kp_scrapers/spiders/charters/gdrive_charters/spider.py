from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.gdrive import GDriveXlsSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.gdrive_charters import normalize


ACCEPTED_HEADERS = [
    'reported_date',
    'provider_name',
    'vessel_name',
    'vessel_imo',
    'vessel_length',
    'vessel_dwt',
    'charterer',
    'status',
    'lay_can_start',
    'lay_can_end',
    'rate_value',
    'rate_raw_value',
    'departure_zone',
    'arrival_zone',
    'cargo_product',
    'cargo_movement',
    'cargo_volume',
    'cargo_unit',
]


class GDriveSpotCharterBase(CharterSpider, GDriveXlsSpider):
    version = '2.0.0'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True
    }

    def process_item(self, item):
        yield normalize.process_item(item)

    def charter_mapping_handler(self, raw_item):
        """check if unknown keys exist in excel file, if it does,
        crash the spider

        """
        extra_headers = set(raw_item.keys()).difference(ACCEPTED_HEADERS)

        if len(extra_headers) > 0:
            raise KeyError(f'foreign key detected {extra_headers}')


class GDriveSpotCharterCOAL(GDriveSpotCharterBase):
    name = 'GDrive_Fixture_COAL'


class GDriveSpotCharterLiquids(GDriveSpotCharterBase):
    name = 'GDrive_Fixture_Liquids'


class GDriveSpotCharterLNG(GDriveSpotCharterBase):
    name = 'GDrive_Fixture_LNG'


class GDriveSpotCharterLPG(GDriveSpotCharterBase):
    name = 'GDrive_Fixture_LPG'
