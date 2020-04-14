from unittest import TestCase

from kp_scrapers.models.enum import Enum


class EnumTestCase(TestCase):
    def setUp(self):
        self.earth = Enum(
            age='4.5 billion years',
            continents=7,
            oceans=['Atlantic', 'Arctic', 'Indian', 'Pacific', 'Southern'],
        )

    def test_get_enum_value(self):
        # given
        earth = self.earth

        # then
        self.assertEqual(earth.continents, 7)
        self.assertNotEqual(earth.continents, '7')
        self.assertEqual(earth.oceans, ['Atlantic', 'Arctic', 'Indian', 'Pacific', 'Southern'])

    def test_modify_existing_enum(self):
        # given
        earth = self.earth

        # then
        with self.assertRaises(AttributeError) as context:
            earth.age = '6000 years'

        self.assertTrue('modify' in str(context.exception))

    def test_add_enum_to_instantiated_object(self):
        # given
        earth = self.earth

        # then
        with self.assertRaises(AttributeError) as context:
            earth.mass = '5,972,000,000,000,000,000,000 tons'

        self.assertTrue('append' in str(context.exception))
