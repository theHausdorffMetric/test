from unittest import TestCase

from kp_scrapers.spiders.port_authorities.bilbao.normalize import normalize_date
from tests._helpers import TestIO, with_test_cases


class BilbaoNormalizeTestCase(TestCase):
    @with_test_cases(
        TestIO(
            'no rollover',
            given={'reported_date': '2018-04-08T00:00:00', 'eta': '11-04'},
            then='2018-04-11T00:00:00',
        ),
        TestIO(
            'rollover year',
            given={'reported_date': '2017-12-31T00:00:00', 'eta': '12-01'},
            then='2018-01-12T00:00:00',
        ),
    )
    def test_normalize_date(self, given):
        return normalize_date(given['eta'], given['reported_date'])
