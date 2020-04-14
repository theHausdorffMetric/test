# -*- coding: utf-8 -*-

"""Pascagoula Spider.

   Scrapes ETA/etd/arrived/departed events
   from port of Pascagoula (PDF source) using tabula.
   webpage : http://www.portofpascagoula.com/Daily.pdf

"""

from __future__ import absolute_import, unicode_literals
from datetime import datetime
import re

from kp_scrapers.lib import utils
from kp_scrapers.models.items import (
    ArrivedEvent,
    Cargo,
    DepartedEvent,
    EtaEvent,
    EtdEvent,
    VesselIdentification,
)
from kp_scrapers.models.utils import filter_item_fields
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


FEET_METER_RATIO = 0.3048


def parse_date(n_date):
    n_date = re.search(r'\d*/\d*/\d*', n_date)
    return datetime.strptime(n_date.group(0), '%m/%d/%Y').date() if n_date else None


def parse_commodity(commo):
    crude_expr = ['LOAD PRODUCT', 'CRUDE']
    crude_match = [re.search(expr, commo.upper()) for expr in crude_expr]
    return ('oil', 'crude') if any(crude_match) else None


def convert_length_to_meter(length):
    return float(length) * FEET_METER_RATIO


def parse_tonnage(ton):
    return float(ton.replace(',', '.')) * 1e3


def keys_map():
    return {
        'Vessel Name': ('name', None),
        'Flag': ('flag', None),
        'Berth': ('berth', None),
        'ETA Pilot Station': ('eta', parse_date),
        'ETD': ('etd', parse_date),
        'Cargo Operation Agent': ('commodity', parse_commodity),
        'Stevedore': utils.ignore_key('blank field in the sheet'),
        'Length': ('length', convert_length_to_meter),
        'Tons': ('dwt', parse_tonnage),
        'Remarks': utils.ignore_key('unused because information not understood'),
    }


def add_keys(row):
    keys = [
        'Vessel Name',
        'Flag',
        'Berth',
        'ETA Pilot Station',
        'ETD',
        'Cargo Operation Agent',
        'Stevedore',
        'Length',
        'Tons',
        'Remarks',
    ]
    return {k: v for k, v in list(zip(keys, row))}


def row_to_dict(row):
    return utils.map_keys(add_keys(row), keys_map())


def strip_row(row):
    return [re.sub(r'\s+$', '', el) for el in row]


def extract_vessel_info(row):
    row_is_vessel_info = None not in row
    return row_to_dict(row) if row_is_vessel_info else None


def extract_reported_date(row):
    is_reported_date = any([re.search('Report Date', el) for el in row])
    return {'reported_date': parse_date(row[0].upper())} if is_reported_date else None


class PascagoulaInfo(object):
    def __init__(self, table):
        self.table = table
        self.row_is_interesting = False

    def _is_interesting_row(self, row):
        is_reported_date = any([re.search('Report Date', el) for el in row])
        if is_reported_date:
            return True
        elif '***CARGO VESSELS***' in row:
            self.row_is_interesting = True
            return False
        elif '***NON-CARGO VESSELS***' in row:
            self.row_is_interesting = False
            return False
        return self.row_is_interesting

    def __next__(self):
        row = next(self.table)
        if self._is_interesting_row(row):
            row = strip_row(row)
            return extract_reported_date(row) or extract_vessel_info(row)
        return None

    def next(self):
        return self.__next__()

    def __iter__(self):
        return self


def extract_arrival_event(info):
    if (info['eta'] - datetime.today().date()).days > 0:
        eta = filter_item_fields(EtaEvent, info)
        EtaEvent(**eta)
    elif info['eta']:
        info['arrival'] = info.pop('eta')
        arrival = filter_item_fields(ArrivedEvent, info)
        return ArrivedEvent(**arrival)
    return None


def extract_departure_event(info):
    if (info['etd'] - datetime.today().date()).days > 0:
        etd = filter_item_fields(EtdEvent, info)
        return EtdEvent(**etd)
    elif info['etd']:
        info['departure'] = info.pop('etd')
        departure = filter_item_fields(DepartedEvent, info)
        return DepartedEvent(**departure)
    return None


def complete_info(info, reported_date, port_name):
    cargo = Cargo(ton=info['dwt'], commodity=info['commodity'])
    info['cargo'] = cargo
    vessel = VesselIdentification(name=info['name'], dwt=info['dwt'], flag=info['flag'])
    info['vessel'] = vessel
    info['reported_date'] = reported_date
    info['port_name'] = port_name
    return info


class PascagoulaSpider(PortAuthoritySpider, PdfSpider):

    name = 'Pascagoula'
    version = '1.0.0'

    start_urls = ['http://www.portofpascagoula.com/Daily.pdf']

    def parse(self, response):
        information = self.extract_pdf_table(response, PascagoulaInfo)
        information = [info for info in information if info is not None]
        # remove last element of information, which is a duplicate reported_date info
        information = information[:-1]
        reported_date, vessels_info = information.pop(-1), information
        for info in vessels_info:
            # refactor vesel data to add all the information that is
            # not originaly present in the rows
            complete_info(info, reported_date, 'pascagoula')
            items = [extract_arrival_event(info), extract_departure_event(info)]
            for item in items:
                yield item
