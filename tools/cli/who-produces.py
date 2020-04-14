#! /usr/bin/env python

import click

from kp_scrapers import __version__, spiders
from kp_scrapers.cli.ui import info, success
from kp_scrapers.lib.services.s3 import upload_blob
from kp_scrapers.pipelines.s3 import ITEMS_BUCKET


def lookup_providers(data_type, use_cache):
    for agent in spiders(use_cache) or []:
        if data_type.lower() in (agent['produces'] or []):
            yield agent


@click.command()
@click.option('--type', 'data_type', required=True)
@click.option('--cache', is_flag=True)
def display(data_type, cache):
    """Display who produces what.

    """
    info(f'looking for data sources exposing `{data_type}` information...')
    for crawler in lookup_providers(data_type, cache):
        success(f"Provider {crawler['provider']} (powered by spider={crawler['name']})")


@click.command()
@click.option('--bucket', default=ITEMS_BUCKET)
@click.option('--filename', default=f'meta.{__version__.replace(".", "-")}.json')
def export(bucket, filename):
    """Export spider attributes to S3.

    This is especially useful for downstream processing where Kpler-aware
    components want to process specific data types, but the datalake only knows
    about sources.

    Usage:

            ./who-produces.py export --bucket kp-datalake

    """
    info(f"exporting spider meta ...")
    key = f'__meta/{filename}'
    upload_blob(bucket, key, spiders(use_cache=False))
    success(f"spider meta is located at `s3://{bucket}/{key}`")


@click.group()
def cli():
    """Display or export spider metadata.
    """
    pass


if __name__ == '__main__':
    cli.add_command(display)
    cli.add_command(export)
    cli()
