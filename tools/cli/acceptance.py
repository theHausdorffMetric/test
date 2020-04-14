#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import sys

import click
from lxml import etree


def assert_results(stats, count):
    if not int(stats.get('tests')) >= count:
        click.secho(
            'acceptance failed: {} tests are not enough'.format(stats.get('tests')),
            fg='red',
            bold=True,
        )
        sys.exit(1)

    # NOTE assert stats.get('skip') <= 10
    # obviously...
    # assert int(stats.get('failures')) == 0
    # assert int(stats.get('errors')) == 0

    click.secho('ok', fg='green', bold=True)


@click.command()
@click.option('-x', '--xunit-files', required=True, multiple=True, help='xunit tests export file')
@click.option(
    '-c', '--count', required=True, type=int, help='acceptance minimum level of tests to run'
)
@click.option('--merge/--no-merge', default=True, help='sum xunit results before asserting them')
def run(xunit_files, count, merge):
    # mock the stats object returned by the parser
    # so that we can use iether of them in `assert_results`
    results = {'tests': 0}

    for test_out in xunit_files:
        with open(test_out, 'rb') as fd:
            parser = etree.iterparse(fd, events=('start', 'end'))

            # we could iterate over that but we only want the stats in the first header
            # NOTE for futur usage maybe, like compute and test the amount of time it took
            event, stats = next(parser)

            results['tests'] += int(stats.get('tests'))

            if not merge:
                assert_results(stats, count)

    if merge:
        assert_results(results, count)


if __name__ == '__main__':
    run()
