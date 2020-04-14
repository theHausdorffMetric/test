from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.gdrive_grades import normalize
from kp_scrapers.spiders.bases.gdrive import GDriveXlsSpider


ACCEPTED_HEADERS = [
    'port_name',
    'eta',
    'berthed',
    'departure',
    'arrival',
    'vessel_name',
    'vessel_imo',
    'cargo_product',
    'cargo_movement',
    'cargo_volume',
    'cargo_unit',
    'cargo_buyer',
    'cargo_seller',
    'provider',
    'reported_date',
    'seller_name',
    'buyer_name',
    'vessel_length',
    'vessel_dwt',
    'provider_name',
]


class GDriveGradesBase(ShipAgentMixin, GDriveXlsSpider):
    version = '2.0.0'
    produces = [DataTypes.CargoMovement, DataTypes.Cargo, DataTypes.Vessel]

    spider_settings = {
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True
    }

    def process_item(self, item):
        self.shipagents_mapping_handler(item)
        yield normalize.process_item(item)

    def shipagents_mapping_handler(self, raw_item):
        """check if unknown keys exist in excel file, if it does,
        crash the spider

        """
        extra_headers = set(raw_item.keys()).difference(ACCEPTED_HEADERS)

        if len(extra_headers) > 0:
            raise KeyError(f'foreign key detected {extra_headers}')


class GDriveGradesCOAL(GDriveGradesBase):
    name = 'GDrive_Grades_COAL'


class GDriveGradesLiquids(GDriveGradesBase):
    name = 'GDrive_Grades_Liquids'


class GDriveGradesLNG(GDriveGradesBase):
    name = 'GDrive_Grades_LNG'


class GDriveGradesLPG(GDriveGradesBase):
    name = 'GDrive_Grades_LPG'
