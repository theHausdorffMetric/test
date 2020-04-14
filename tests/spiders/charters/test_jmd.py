from datetime import datetime
import unittest

from kp_scrapers.spiders.charters.jmd import (
    parse_areas,
    parse_contract,
    parse_dwt,
    parse_loading_date,
    parse_vessel_name,
)


CONTRACTS_TABLE = [
    ['UNIPEC', ('UNIPEC', 'Fully Fixed')],
    ['CSSSA - FLD', ('CSSSA', 'Failed')],
    ['MERCURIA - REPL', ('MERCURIA', 'Replaced')],
    ['CHEMCHINA - UPDATE', ('CHEMCHINA', 'Updated')],
    ['BRIGHTOIL - CORR', ('BRIGHTOIL', 'Fully Fixed')],
    ['BRIGHTOIL - RPTD', ('BRIGHTOIL', 'Fully Fixed')],
    ['BRIGHTOIL - RCNT', ('BRIGHTOIL', 'Fully Fixed')],
    ['BRIGHTOIL - RPLC', ('BRIGHTOIL', 'Replaced')],
]

AREAS_TABLE = [
    ['KUWAIT / EAST', ('KUWAIT', ['Eastern Asia'])],
    ['SERIA + / THAI', ('SERIA', ['THAI'])],
    ['B.URIP/TJP+THAI', ('B.URIP', ['TJP'])],
]

NAMES_TABLE = [
    ['(01) MARAN CASTOR', 'MARAN CASTOR'],
    ['AMCL TBN', 'AMCL TBN'],
    ['DUBAI GLAMOUR O/O', 'DUBAI GLAMOUR'],
]


class JMDSpiderTestCase(unittest.TestCase):
    def test_parse_vessel_name(self):
        for name in NAMES_TABLE:
            self.assertEqual(parse_vessel_name(name[0]), name[1])

    def test_parse_dwt(self):
        self.assertEqual(parse_dwt(u'\xa0'), None)
        self.assertEqual(parse_dwt('270/280'), 275000)
        self.assertEqual(parse_dwt('270'), 270000)

    def test_parse_loading_date(self):
        lay_can_start = datetime(datetime.now().year, 3, 18, 0, 0, 0).isoformat()
        lay_can_end = datetime(datetime.now().year, 3, 20, 0, 0, 0).isoformat()
        self.assertEqual(parse_loading_date('3 / 18-20'), (lay_can_start, lay_can_end))

    def test_parse_contract(self):
        self.assertEqual(parse_contract(u'\xa0'), (None, None))
        for contract in CONTRACTS_TABLE:
            self.assertEqual(parse_contract(contract[0]), contract[1])

    def test_parse_areas(self):
        for area in AREAS_TABLE:
            self.assertEqual(parse_areas(area[0]), area[1])
