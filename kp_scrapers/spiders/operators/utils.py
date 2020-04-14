# -*- coding: utf8 -*-

"""Units conversion methods.

General idea: Value is none if its impossible to convert
"""

from __future__ import unicode_literals


UNITS = ['M3', 'GWH', 'MWH', 'KWH']


class UnitException(Exception):
    pass


def unit_exist(unit_str):
    for unit in UNITS:
        upper = unit_str.upper()
        if upper == unit:
            return unit

    raise UnitException('Unit {} does not exist to me'.format(unit_str))


def convert_to_unique_unit(val, str_unit):
    if str_unit == 'GWH':
        return (val * pow(10, 6), 'KWH')
    if str_unit == 'MWH':
        return (val * pow(10, 3), 'KWH')

    return (val, str_unit)


def kwh_from_km3lng(number):
    exp_ratio = 570
    return float(number * (10 ** 9 * exp_ratio) / (3412 * 24.36))
