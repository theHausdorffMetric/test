# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from uuid import uuid4

from kp_scrapers import __version__ as _package_version


def strip_meta_fields(item):
    """Expose all Scrapy item fields but the one of the context base class.

    Args:
        item(dict): raw arbitrary dict

    Examples:
        >>> strip_meta_fields({'kp_package_version': '1.2.3', 'foo': 'bar'})
        {'foo': 'bar'}

    """
    fields_to_strip = dir(VersionedItem)
    return {k: v for k, v in item.items() if k not in fields_to_strip}


class VersionedItem(dict):
    """Common denominator of Kpler items, or more broadly sane raw data
    primitive.

    This base class tries to provide metadata for further tooling down the
    stream, while still being as agnostic as possible with what we want to do
    with it.

    One can just inherit from it and transparently have the metadata
    filled at initialization:


            from scrapy.item import Item, Field
            from kp_scrapers.models.base import VersionedItem

            class MyscrapyItem(VersionedItem, Item):
                foo = Field()


    Or extend raw json information:


            from kp_scrapers.models.base import VersionedItem

            data =  VersionedItem(foo='bar')


    ///////////////////////////////////////////////////////////////////////

    With the release of the raw pipeline this class is basically ensuring
    the transition from legacy items to new normalized ones.
    Legacy items are mapped during scraping and not pushed, they don't have a
    `normalisation` module. Hence once queried from S3 they are served as is to
    the caller.

    ///////////////////////////////////////////////////////////////////////

    """

    # this version is expected to be bumped with every change in the repo,
    # since it follows semantic versioning of the project.
    kp_package_version = None

    # identity tracking across the stack
    kp_uuid = None
    # opt-in feature: allow to define some kind of links/hierarchy
    # between items; the definition of this field makes possible to pass a
    # reference when creating the data. Otherwise it won't exists and yields a
    # `KeyError` exception when trying to `item['kp_parent_uuid']`.
    kp_parent_uuid = None

    # data source provider code we can use to identify source across the stack
    # we can sketch hierarchy of a data item as follow
    #
    #
    #   provider: external, unstructured data source (api, website, ...)
    #       |
    #       |___ spider1: an agent that extracts everything it can from this provider
    #       |
    #       |___ spider2           |---- item1
    #               |______________|
    #                              |____ item2: event type (Position, PortCall)
    #                                       |
    #                                       |_ field1
    #                                       |_ field2
    #                                       |_ struct1: static data information (Vessel, Port, ...)
    #                                       |     |___ field1
    #                                       |     |___ field2
    #                                       |
    #                                       |_ struct2
    #                                             |___ field1
    #
    #
    #
    # TODO kp_data_provider = None
    provider_name = None  # as currently understood by the ETL

    # scrapy magic fields - will be set automatically by the extension
    sh_job_time = None
    sh_spider_name = None
    sh_job_id = None
    sh_item_time = None

    def __init__(self, *args, **kwargs):
        # initialize dictionary magic with whatever `key:value` given
        super(VersionedItem, self).__init__(*args, **kwargs)

        # default setup for traceable blobs of data
        self['kp_uuid'] = str(uuid4())
        self['kp_package_version'] = _package_version
