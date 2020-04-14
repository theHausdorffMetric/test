from unittest import TestCase

from googletrans import Translator

from tests._helpers import TestIO, with_test_cases


class DamiettaGoogletransTestCase(TestCase):
    @with_test_cases(TestIO('arabic test', given='شركة الشحن', then='Shipping company'))
    def test_googletrans_api(self, given):
        translator = Translator()
        return translator.translate(given).text
