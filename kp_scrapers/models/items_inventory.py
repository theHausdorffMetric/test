# -*- coding: utf-8 -*-

from __future__ import absolute_import

from scrapy.item import Field, Item

from kp_scrapers.models.base import VersionedItem


class IOItem(VersionedItem, Item):
    input_forecast_o = Field()
    output_forecast_o = Field()
    output_o = Field()
    input_o = Field()
    inflow_o = Field()
    outflow_o = Field()
    output_cargo = Field()
    input_cargo = Field()
    net_flow_o = Field()
    unit = Field()
    # Timestamp of the last data update. If not provided by the scraped
    # document, leave unset, the scraping timestamp will be used.
    date = Field()
    src_file = Field()
    level = Field()
    level_o = Field()
    destinations = Field()
    sources = Field()
    city = Field()
