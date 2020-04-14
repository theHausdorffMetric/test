from __future__ import absolute_import, print_function, unicode_literals
import json
from pprint import pprint as pp

from scrapy.commands import ScrapyCommand


class Command(ScrapyCommand):

    requires_project = True
    default_settings = {'LOG_ENABLED': False}

    def short_desc(self):
        return "List spiders and their properties"

    def syntax(self):
        return "[options] <spiders>"

    @property
    def spider_klasses(self):
        spider_names = self.crawler_process.spider_loader.list()
        loader = self.crawler_process.spider_loader.load
        return list(map(lambda s: loader(s), spider_names))

    def add_options(self, parser):
        ScrapyCommand.add_options(self, parser)
        # TODO support multiple filters
        parser.add_option(
            "-f",
            "--filter",
            help="limit output to what match the given k=v",
            action="append",
            default=[],
        )
        parser.add_option(
            "-S",
            "--silent",
            default=False,
            action="store_true",
            help="no output on the stdout, for use in other cli tools",
        )
        parser.add_option(
            "--prettify", default=False, action="store_true", help="Pretty print output"
        )
        parser.add_option("--export", default=None, help="results output redirection")

    def run(self, args, opts):
        # Scrapy doesn't support a generator as a `run` function for commands,
        # so we are bac to good old ugly empty init
        metas = []

        spiders_constraint = args if len(args) else []

        for spider in self.spider_klasses:
            if spiders_constraint and spider.name not in spiders_constraint:
                continue

            # TODO fix all spiders. This hacky skip is meant to be removed
            # FIXME customs overwrite the `commodities` function
            try:
                commodities = spider.commodities()
            except (NotImplementedError, AttributeError, TypeError):
                commodities = []

            data_types = spider.produces
            if not isinstance(data_types, list):
                # usually not implemented by the spider and then this is a type `property`
                data_types = []

            try:
                category = spider.category()
            except (NotImplementedError, AttributeError):
                category = 'unknown'

            # the `DeprecatedMixin` set this attribute to true
            enabled = not getattr(spider, 'deprecated', False)
            need_java = hasattr(spider, 'extract_pdf_table')

            ignore = False
            for filters in opts.filter:
                k, v = filters.split(':')
                if k == 'category' and v != category:
                    ignore = True
                elif k == 'name' and v != spider.name:
                    ignore = True
                elif k == 'commodities' and v not in commodities:
                    ignore = True
                elif k == 'produces' and v not in data_types:
                    ignore = True
                elif k == 'enabled' and v.title() != str(enabled):
                    ignore = True
                elif k == 'need_java' and v.title() != str(need_java):
                    ignore = True
            if ignore:
                continue

            # NOTE we don't filter over `args` and `kwargs` as they represent
            # an hint that more arguments can be received
            spider_cli = [
                init_arg
                for init_arg in spider.__init__.__code__.co_varnames
                if init_arg not in ['self']
            ]

            # output machine-readable information (hence the casting)
            spider_metas = {
                '_type': 'spider',
                'doc': spider.__doc__ or spider.__init__.__doc__,
                'cli_options': spider_cli,
                'name': spider.name,
                'version': spider.version if isinstance(spider.version, str) else None,
                'provider': spider.provider if isinstance(spider.provider, str) else None,
                'produces': spider.produces if isinstance(spider.produces, list) else None,
                # extracting pdf document currently requires Tabula, a java
                # project, to be installed
                'need_java': need_java,
                'category': category,
                'commodities': list(commodities),
                'enabled': enabled,
            }

            # either it goes on stdout or we expose it for programs
            if not opts.silent:
                if opts.prettify:
                    pp(spider_metas)
                else:
                    # json lines, for machines
                    print(json.dumps(spider_metas))

            # expose results to programmatic use of the class
            # `scrapy describes` will ignore those values
            metas.append(spider_metas)

        if opts.export:
            with open(opts.export, 'w') as fd:
                json.dump(metas, fd)

        return metas
