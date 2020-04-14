from collections import defaultdict

from kp_scrapers.cli.ui import gauge_dict, is_terminal
from kp_scrapers.lib.utils import flatten_dict


class ReportStats(object):
    """Rolling computation of item structure statistics."""

    # link nested keys (same as `flatten_dict` default)
    separator = '.'

    def __init__(self, terminal_flag):
        self.terminal_flag = terminal_flag

    @classmethod
    def from_crawler(cls, crawler):
        # NOTE could if not crawler.settings.getbool('KP_REPORTER_ENABLED'):
        if is_terminal():
            # NOTE we could hook this plugin to datadog on production
            # like setting a custom stats and DD extension could pick every
            # stat that starts with a specific prefix
            return cls(terminal_flag=True)
        else:
            return cls(terminal_flag=False)

    def open_spider(self, spider):
        """Initialize internal metrics."""
        # we don't have yet any knowledge of items
        # NOTE later on we could make use of the `produces` attribute and
        # detect anomalies
        self.stats = defaultdict(lambda: 0)

        # NOTE could be a spider setting?
        self.blacklisted_value = None

    def close_spider(self, spider):
        total_items = spider.crawler.stats.get_value('item_scraped_count')
        # include spider attributes in stats to be used in redshift monitoring module
        spider.crawler.stats._stats['spider_attribute_stats'] = self.stats
        # check if terminal flag is true, if true pretty print stats to terminal
        if self.terminal_flag:
            gauge_dict(self.stats, total_items)

    def process_item(self, item, spider):
        """Extend item context.
        Automate how we initialize kpler items.  this step allows developers to
        only extract arbitrary key/values from.  data sources, while meta
        information is populated consistently here
        It also reuses the base class developed to transition from old Scrapy
        items to raw dicts. Reducing risks of errors, factorizing code and
        helping a consistent migration.
        """
        # TODO option to ignore metas like `kp_*`?
        # TODO support schematics Models
        if isinstance(item, dict):
            for k, v in flatten_dict(item).items():
                # `flatten_dict` doesn't support nested arrays, but here we can handle it
                if isinstance(v, list):
                    for nested_blob in v:
                        # on purpose, we only support 1 level of depth for nested objects
                        for nested_k, nested_v in flatten_dict(nested_blob).items():
                            if nested_v != self.blacklisted_value:
                                flat_key = k + self.separator + nested_k
                                self.stats[flat_key] += 1
                elif v != self.blacklisted_value:
                    self.stats[k] += 1

        return item
