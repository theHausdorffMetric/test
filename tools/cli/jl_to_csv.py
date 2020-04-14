#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""Convert json lines output to csv."""

from __future__ import absolute_import, unicode_literals

import click
import simplejson as json
from six.moves import map


def flatten_export(item):
    # don't like to mutate innocent things
    charterer = item.copy()

    vessel = charterer.pop('vessel')
    charterer.update(vessel)

    # declutter for analysts
    for k, v in item.items():
        if k.startswith('_') or k.startswith('sh_'):
            charterer.pop(k)

    return charterer


# TODO use click File type
@click.command()
@click.argument('jl_file')
@click.option('-o', '--out', default='export.csv', help='csv export filename')
@click.option('--sep', default=',', help='csv export separator')
def convert(jl_file, out, sep):
    """Convert json lines formatted file into CSV.

    This is especially handy for sharing local spider output with analysts who
    use Excel.

    """
    header = None
    # //// change this line if you want to apply some kind of mapping ////
    transform = lambda x: x  # noqa

    with open(jl_file) as fd_in:
        click.secho('loading original data', fg='yellow')

        data = [json.loads(line) for line in fd_in]

        with open(out, 'w') as fd_out:
            clean_data = map(transform, data)
            for row in clean_data:
                if header is None:
                    header = sep.join(row.keys())
                    fd_out.write(header + '\n')
                else:
                    csv_row = sep.join(map(lambda x: str(x), row.values()))
                    fd_out.write(csv_row + '\n')

    click.secho('exported csv to `{}`'.format(out), fg='green')


if __name__ == '__main__':
    convert()
