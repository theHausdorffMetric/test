# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
from unittest import TestCase

from kp_scrapers.spiders.bases.markers import (
    CoalMarker,
    KplerMixin,
    LngMarker,
    LpgMarker,
    OilMarker,
)


class CommodityMakerTestCase(TestCase):
    def test_coal_maker(self):
        """Ensure class inheriting from CoalMarker has commodity 'lng'
        """

        class C(CoalMarker):
            pass

        res = C.commodities()
        self.assertEqual(sorted(res), ['coal'])

    def test_lng_maker(self):
        """Ensure class inheriting from LngMarker has commodity 'lng'
        """

        class C(LngMarker):
            pass

        self.assertEqual(C.commodities(), ['lng'])

    def test_lpg_maker(self):
        """Ensure class inheriting from LpgMarker has commodity 'lpg'
        """

        class C(LpgMarker):
            pass

        self.assertEqual(C.commodities(), ['lpg'])

    def test_oil_maker(self):
        """Ensure class inheriting from OilMarker has commodity 'oil'
        """

        class C(OilMarker):
            pass

        self.assertEqual(C.commodities(), ['oil'])

    def test_multiple_inheritance_maker(self):
        """Ensure inheriting from several commodity marker classes work as expected
        """

        class C(CoalMarker, LngMarker, LpgMarker, OilMarker):
            pass

        res = C.commodities()
        self.assertEqual(sorted(res), ['coal', 'lng', 'lpg', 'oil'])


class KplerSpiderTestCase(TestCase):
    def test_category_raise_not_implemented_error_if_not_overriden(self):
        """Ensure the category method raises a NotImplementedError
        """
        with self.assertRaises(NotImplementedError):
            KplerMixin.category()

    def test_commodities_raise_not_implemented_error_if_not_overriden(self):
        """Ensure the commodities method raises a NotImplementedError
        """
        with self.assertRaises(NotImplementedError):
            KplerMixin.commodities()
