# -*- coding: utf-8; -*-

"""The purpose of this module is to provide means to quickly and easily
provide metadata on spider.

The first important piece this module defines is a contract interface,
any spider should implement to be *describable*: :class:`.KplerMixin`.
Obviously this class does not provide any concrete implementation.

The second piece this modules defines is several marker classes that
can be used to implement parts the contract defined by the :class:`.KplerMixin`
class. By marker class we mean class your spiders only need to inherit
from to get the benefit of fulfilling a part of the contract.

As an example the :class:`.CoalMarker`, :class:`.LngMarker`,
:class:`.LngMarker`, :class:`.OilMarker` classes that implement the
:meth:`~kp_scrapers.spiders.bases.markers.KplerMixin.commodities` part of the
contract. You can inherit from one or several of them, each will add the
corresponding commodity to the list of commodities your spider is concerned
about.

"""

from __future__ import absolute_import, unicode_literals
import os

from kp_scrapers.cli.ui import warning_banner
from kp_scrapers.lib.errors import break_contract, SpiderShouldNotRun


try:
    from enum import Enum
except ImportError:
    from enum34 import Enum


FAILURE_CODE = 1


__all__ = [
    'CoalMarker',
    'CppMarker',
    'KplerMixin',
    'LngMarker',
    'LpgMarker',
    'OilMarker',
    'SpiderCategory',
]


class SpiderCategory(Enum):
    """Enumeration of the categories a spider may belong to.

    A spider may only belong to a single category.
    """

    ais = 'ais'
    # shall we considere it as a `registry` kind ?
    ais_fleet = 'ais-fleet'
    canal = 'canal'
    charter = 'charter'
    contract = 'contract'
    custom = 'custom'
    operator = 'operator'
    port_authority = 'port-authority'
    price = 'price'
    registry = 'registry'
    ship_agent = 'ship-agent'
    slot = 'slot'
    weather = 'weather'


class KplerMixin(object):
    """Contract interface for introspectable Spiders
    """

    category_settings = None
    spider_settings = None

    @classmethod
    def category(cls):
        """Returns the category of the spider.

        Returns:
            str: the category of the spider.

        TODO:
            use the enum instead of strings.
        """
        break_contract(cls.__name__, 'category() -> str')

    @classmethod
    def commodities(cls):
        """Returns the list of commodities a spider is concerned with.

        Returns:
            List[str]: A list containing the names of the commodities
                a spider retrieves data for.
        """
        break_contract(cls.__name__, 'commodities() -> List[str]')

    @property
    def version(self):
        """Returns the development version of a specific spider."""
        warning_banner('`version` property will soon be mandatory - start migrating')

    @property
    def provider(self):
        """Returns the data provider name of a specific spider."""
        warning_banner('`provider` property will soon be mandatory - start migrating')

    @property
    def produces(self):
        """Returns the data types a spider extracts."""
        warning_banner('`produces` property will soon be mandatory - start migrating')

    @staticmethod
    def abort(code=FAILURE_CODE):
        os._exit(FAILURE_CODE)

    @classmethod
    def update_settings(cls, settings):
        """Dynamically set spider common settings.

        This methods overwrites built-in Scrapy one which just use
        `custom_settings` to update project settings before it becomes
        immutable.

        We supercharge this behaviour with different granularity, namely
        spider_settings and category_settings, allowing developers to set
        settings based on scope withput overwriting parent concerns.

        """
        # re-implement same behaviour than initial `Scrapy` implementation
        settings.setdict(cls.custom_settings or {}, priority='spider')
        # customize further
        settings.setdict(cls.category_settings or {}, priority='spider')
        settings.setdict(cls.spider_settings or {}, priority='spider')

        # customize datadog hostname so we can attribute a different hostname
        # per scraper. beyond analysis it enables powerful dataviz like
        # hostmap
        project_id = os.getenv('SCRAPY_PROJECT_ID', 'production')
        hostname = '-'.join([cls.name.lower(), project_id, 'scraping'])
        settings.set('DATADOG_HOST_NAME', hostname, priority='spider')


class DeprecatedMixin(object):
    """Contract interface to hide deprecated spiders.

    It tries to prevent them from running and appearing in
    Scrapy list.

    """

    # add the property to the unfortunate spider
    deprecated = True

    def __new__(cls, *args, **kwargs):
        raise SpiderShouldNotRun(spider_name=cls.__name__, reason='deprecated')


class _CommodityMarker(KplerMixin):
    """Base class for all commodity marker classes.

    Its main purpose is to be a sentinel against calling
    :meth:`KplerMixin.commodities` which would raise a :class:`NotImplementedError`
    exception.
    """

    @classmethod
    def commodities(cls):
        return []


class CoalMarker(_CommodityMarker):
    """Marker class to inherit from if your spider scraps data about Coal
    """

    @classmethod
    def commodities(cls):
        """Appends coal to the list of commodities a spider is concerned with.

        Returns:
            List[str]: A list containing ``'coal'`` and the other commodities
                a spider retrieves data for, if any.
        """
        markers = ['coal']
        markers.extend(super(CoalMarker, cls).commodities())
        return markers


class CppMarker(_CommodityMarker):
    """Marker class to inherit from if your spider scraps data about CPP
    """

    @classmethod
    def commodities(cls):
        """Appends `'cpp'` to the list of commodities a spider is concerned with.

        Returns:
            List[str]: A list containing ``'cpp'`` and the other commodities
                a spider retrieves data for, if any.
        """
        markers = ['cpp']
        markers.extend(super(CppMarker, cls).commodities())
        return markers


class LngMarker(_CommodityMarker):
    """Marker class to inherit from if your spider scraps data about Lng
    """

    @classmethod
    def commodities(cls):
        """Appends `'lng'` to the list of commodities a spider is concerned with.

        Returns:
            List[str]: A list containing ``'lng'`` and the other commodities
                a spider retrieves data for, if any.
        """
        markers = ['lng']
        markers.extend(super(LngMarker, cls).commodities())
        return markers


class LpgMarker(_CommodityMarker):
    """Marker class to inherit from if your spider scraps data about Lpg
    """

    @classmethod
    def commodities(cls):
        """Appends `'lpg'` to the list of commodities a spider is concerned with.

        Returns:
            List[str]: A list containing ``'lpg'`` and the other commodities
                a spider retrieves data for, if any.
        """
        markers = ['lpg']
        markers.extend(super(LpgMarker, cls).commodities())
        return markers


class OilMarker(_CommodityMarker):
    """Marker class to inherit from if your spider scraps data about Oil
    """

    @classmethod
    def commodities(cls):
        """Appends `'oil'` to the list of commodities a spider is concerned with.

        Returns:
            List[str]: A list containing ``'oil'`` and the other commodities
                a spider retrieves data for, if any.
        """
        markers = ['oil']
        markers.extend(super(OilMarker, cls).commodities())
        return markers
