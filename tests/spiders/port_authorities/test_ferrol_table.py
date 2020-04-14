# -*- coding: utf-8; -*-

from __future__ import absolute_import, print_function, unicode_literals
from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock

from kp_scrapers.spiders.port_authorities.ferrol import FerrolTable
from tests._helpers.mocks import fixtures_path


class FerrolTableTestCase(TestCase):
    def _get_table(self, name):
        filename = fixtures_path('port_authorities', 'ferrol', name)
        with open(filename, 'r') as f:
            table = FerrolTable(f.read(), filename, Mock())  # Spider
        return table

    def test_ferrol_autorized_line_with_no_comodity_no_quantity(self):
        table = self._get_table('AUTORIZADOS-no_comodity_no_quantity.txt')
        table._lineno, table._line = next(table._content)

        res = table._parse_line()

        self.assertEqual(
            res,
            {
                'berth': 'NAVANTIA FERROL DIQUE 3',
                'vessel_name': 'BRITISH TRADER',
                'event': 'REPARACION',
                'eta': datetime.strptime('19.02.15 09:30', '%d.%m.%y %H:%M'),
            },
        )

    def test_ferrol_authorized_line_no_mooring_post(self):
        table = self._get_table('AUTORIZADOS-no_mooring_post.txt')
        table._lineno, table._line = next(table._content)

        res = table._parse_line()

        self.assertEqual(
            res,
            {
                'berth': 'FONDEO ARES',
                'vessel_name': 'ASTREA',
                'cargo_ton': 1497.0,
                'cargo_type': 'ACERO',
                'event': 'E',
                'eta': datetime.strptime('30.03.2015 05:00', '%d.%m.%Y %H:%M'),
            },
        )

    def test_ferrol_reparation_no_comodity_qty_no_mooring_posts(self):
        table = self._get_table('AUTORIZADOS-no_comodity_qty_no_mooring_post.txt')
        table._lineno, table._line = next(table._content)

        res = table._parse_line()

        self.assertEqual(
            res,
            {
                'berth': 'NAVANTIA FENE M 11',
                'vessel_name': 'BRUGGE VENTURA',
                'event': 'REPARACION',
                'eta': datetime.strptime('01.03.2015 18:00', '%d.%m.%Y %H:%M'),
            },
        )

    def test_parse_autorized_port_call_file(self):
        filename = fixtures_path('port_authorities', 'ferrol', 'AUTORIZADOS 19.02.15.txt')
        with open(filename, 'r') as f:
            table = FerrolTable(f.read(), filename, Mock())

        res = table.parse()
        for item in res:
            print('*** Result: ', item)

    def test_parse_initiated_port_call_file(self):
        filename = fixtures_path('port_authorities', 'ferrol', 'INICIADOS 19.02.15.txt')
        with open(filename, 'r') as f:
            table = FerrolTable(f.read(), filename, Mock())

        res = table.parse()
        for item in res:
            print('*** Result: ', item)

    def test_parse_initiated_port_call_file2(self):
        filename = fixtures_path('port_authorities', 'ferrol', 'INICIADOS 24.02.2015.txt')
        with open(filename, 'r') as f:
            table = FerrolTable(f.read(), filename, Mock())

        res = table.parse()
        for item in res:
            print('*** Result: ', item)

    def test_parse_terminated_port_call_file(self):
        filename = fixtures_path('port_authorities', 'ferrol', 'FINALIZADOS 19.02.15.txt')
        with open(filename, 'r') as f:
            table = FerrolTable(f.read(), filename, Mock())

        res = table.parse()
        for item in res:
            print('*** Result: ', item)

    def test_parse_port_call_enquiry_file(self):
        filename = fixtures_path('port_authorities', 'ferrol', 'SOLICITADOS 19.02,15.txt')
        with open(filename, 'r') as f:
            table = FerrolTable(f.read(), filename, Mock())

        res = table.parse()
        for item in res:
            print('*** Result: ', item)

    def test_ferror_float_joined_to_text_regression(self):
        table = self._get_table('AUTORIZADAS-no-blank-between-float-and-text.txt')
        table._lineno, table._line = next(table._content)

        res = table._parse_line()
        self.assertEqual(
            res,
            {
                'berth': 'SAN CIPRIAN MUELLE PRINCIPAL',
                'vessel_name': 'GENCO KNIGHT',
                'eta': datetime.strptime('28-06-2015 17:00', '%d-%m-%Y %H:%M'),
                'cargo_type': 'BAUXITA',
                'cargo_ton': 630053.0,
                'event': 'D',
            },
        )

    def test_ferrol_iniciados_parenthesis_in_berth_name(self):
        table = self._get_table('INICIADOS-parenthesis-in-berth.txt')
        table._lineno, table._line = next(table._content)

        res = table._parse_line()
        self.assertEqual(
            res,
            {
                'berth': 'FERNANDEZ LADREDA (1 - 18)',
                'vessel_name': 'ASTREA',
                'event': 'E',
                'cargo_type': 'REBARS',
                'cargo_ton': 2989.0,
                'departure_destination': 'PORTUGAL',
                'arrival_date': datetime.strptime('28-06-2015 22:00', '%d-%m-%Y %H:%M'),
            },
        )

    def test_ferrol_authorized_find_first_line_regression(self):
        table = self._get_table('AUTORIZADAS 08.04.15.txt')
        # table._lineno, table._line = next(table._content)

        table._find_first_line()

        self.assertEqual(table._lineno, 3)

    # Corner cases we may want to cope with but cannot so far

    def test_parse_autorized_with_no_operation_and_no_mooring_posts(self):
        filename = 'AUTORIZADOS-no_operation_no_mooring_posts.txt'

        table = self._get_table(filename)
        table._lineno, table._line = next(table._content)
        pristine_line = table._line
        pristine_lineno = table._lineno

        table._parse_line()

        # should never be reached
        table.logger.error.assert_any_call(
            '{}:{}: could not parse line (see exception below): "{}"'.format(
                filename, pristine_lineno, pristine_line
            )
        )
