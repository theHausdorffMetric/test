# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from abc import abstractmethod

import six

from kp_scrapers.lib.date import get_month_look_back
from kp_scrapers.spiders.customs import CustomsSpider


# FIXME: for now consider only lng so that it does not change default behaviour
#        until we are sure
DEFAULT_COMMOS = ['lng']


class CustomsBaseSpider(CustomsSpider):
    def __init__(self, months_look_back=None, start_date=None, commodity=None, *args, **kwargs):
        super(CustomsSpider, self).__init__(*args, **kwargs)
        self.months_look_back = get_month_look_back(months_look_back, start_date)

        if commodity is not None:
            self._commodities = [c for c in commodity.split(' ') if c] or DEFAULT_COMMOS
        else:
            self._commodities = DEFAULT_COMMOS

    # NOTE what is the purpose of this method ?
    @abstractmethod
    def commodity_mapping(self):
        pass

    def relevant_commodities(self):
        return {
            key: values
            for key, values in six.iteritems(self.commodity_mapping())
            if key in self._commodities
        }

    @property
    def products(self):
        products_codes = dict()
        for commodity, codes in six.iteritems(self.relevant_commodities()):
            for code, code_name in six.iteritems(codes):
                if code in products_codes:
                    if code_name in products_codes[code]:
                        products_codes[code][code_name].append(commodity)
                    else:
                        products_codes[code][code_name] = [commodity]
                else:
                    products_codes[code] = {code_name: [commodity]}
        return products_codes

    def get_product(self, value):
        for code in self.products:
            if code in value:
                return code
        return None

    def get_subcommodities(self, code):
        subcommodities = dict()
        for code_name, commodities in six.iteritems(self.products[code]):
            if len(commodities) > 1:
                raise Exception(
                    'Multiple commodities {} linked to product {} ({})'.format(
                        commodities, code_name, code
                    )
                )
            subcommodities[code_name] = commodities[0]
        return subcommodities
