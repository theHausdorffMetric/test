# -*- coding: utf-8 -*-

"""All the User interface things."""

from __future__ import absolute_import, unicode_literals
import logging
import os
import subprocess
import sys

import click
from emoji import emojize
from tqdm import tqdm

from kp_scrapers.lib.utils import run_once


logger = logging.getLogger(__name__)


# http://www-numi.fnal.gov/offline_software/srt_public_context/WebDocs/Errors/unix_system_errors.html  # noqa
EPERM_CODE = 1
# useful for slack messages
SUCCESS_COLOR = '#4BB543'
ERROR_COLOR = '#F00'


def is_terminal():
    return (
        # scrapinghub binds `stdout` to `StdoutLogger` which doesn't implement istty
        hasattr(sys.stdout, 'isatty')
        and sys.stdout.isatty()
    )


def info(msg):
    click.secho(f"[ .. ] {msg}", bold=True, fg='yellow')


def success(msg):
    click.secho(f"[ {emojize(':heavy_check_mark:')}  ] {msg}", bold=True, fg='green')


def fail(msg):
    click.secho(f"[ {emojize(':x:', use_aliases=True)} ] {msg}", bold=True, fg='red')


def fatal(msg, exit_code=EPERM_CODE):
    fail(msg)
    # exit violently. Spiders can be hard to stop, use it at your own risks and
    # when things should seriously stop running
    os._exit(exit_code)


@run_once
def warning_banner(msg):
    _pretty_warning(msg) if is_terminal() else logger.warning(msg)


def _pretty_warning(msg):
    click.secho(
        """

   ╭────────────────────────────────────────────────────────────────────────────╮
   │                                                                            │
   │   {:^70}   │
   │                                                                            │
   ╰────────────────────────────────────────────────────────────────────────────╯

   """.format(
            "warning: " + msg
        ),
        bold=True,
        fg='red',
    )


def hr(symbol='-', **opts):
    """Print `symbol` across the terminal.

    Args:
        opts(dict): click.secho formatting options

    """
    _, columns = subprocess.check_output(['stty', 'size']).decode().split()
    click.secho(symbol * int(columns), **opts)


def gauge_dict(stats, total):
    for k, stat in stats.items():
        aligned_key = '{0: <30}'.format(k)
        tqdm(total=total, desc=aligned_key, initial=stat, unit='items')
