# -*- coding: utf-8 -*-


# NOTE it actually has a serious downside. If one wants to import a small
# command, it will bring all those scripts as well
from __future__ import absolute_import, unicode_literals

# for package API: `from kp_scrapers.cli.commands import export`
from kp_scrapers.cli.commands.export import export  # noqa
from kp_scrapers.cli.commands.manage import create, delete  # noqa
from kp_scrapers.cli.commands.misc import browse, check  # noqa
from kp_scrapers.cli.commands.schedule import batch_retire, batch_schedule, schedule, scrape  # noqa
from kp_scrapers.cli.commands.tags import manage_tags  # noqa
