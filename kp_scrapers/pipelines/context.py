# -*- coding: utf-8 -*-

"""Item enrichment pipeline.

Contains pipeline class for adding source metadata to each item.

"""

from __future__ import absolute_import

from kp_scrapers.models.base import VersionedItem


class EnrichItemContext(object):
    def process_item(self, item, spider):
        """Extend item context.

        Automate how we initialize kpler items.  this step allows developers to
        only extract arbitrary key/values from.  data sources, while meta
        information is populated consistently here

        It also reuses the base class developed to transition from old Scrapy
        items to raw dicts. Reducing risks of errors, factorizing code and
        helping a consistent migration.

        """
        if isinstance(item, dict):
            # copy actual information
            # NOTE `item` may already contain `kp_source` metafields from models
            #      therefore, we update `v_item` first before returning it
            v_item = VersionedItem(**item)
            v_item.update(
                # add spider meta-attributes
                # class init will do the rest
                kp_source_version=spider.version,
                kp_source_provider=spider.provider,
            )
            return v_item

        # otherwise don't interfere, this is probably a legacy item that just works
        return item
