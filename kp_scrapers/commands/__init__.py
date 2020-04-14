# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import optparse

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from kp_scrapers.commands import describe


def run_scrapy_command(cli_cmd, cli_opts):
    """Simplified version of how `scrapy <command>` works."""
    settings = get_project_settings()
    # since this is not a cli function args are not necessary. But it keeps the
    # behaviour consistent with `scrapy <command>`
    parser = optparse.OptionParser(
        formatter=optparse.TitledHelpFormatter(), conflict_handler='resolve'
    )

    if cli_cmd == 'describe':
        cmd = describe.Command()
    else:
        raise NotImplementedError("`{}` is not a valid command".format(cli_cmd))

    cmd.settings = settings
    cmd.add_options(parser)

    opts, args = parser.parse_args(args=cli_opts.split())

    cmd.crawler_process = CrawlerProcess(settings)
    return cmd.run(args, opts)
