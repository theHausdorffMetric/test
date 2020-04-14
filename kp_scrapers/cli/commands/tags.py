# -*- coding: utf-8 -*-

import click

from kp_scrapers.cli.utils import fetch_jobs, has_scraped_data, search_opts
from kp_scrapers.lib.services import shub


CONSUMED_TAGS = [
    'consumed',  # lng production
    'consumed_server_lpg',  # lpg production
    'consumed_oil_testing',  # oil production
    'consumed_cpp_production',
    'consumed_coal_production',
    'consumed_staticdata_production',
]


@click.command('manage-tags')
@search_opts
@click.option('-a', '--add', 'add_tags', multiple=True, help="tags to add to the jobs")
@click.option('-r', '--remove', 'remove_tags', multiple=True, help="tags to remove to the jobs")
@click.argument('scenario', required=False)
def manage_tags(scenario, add_tags, remove_tags, **opts):
    """Mutate Scrapinghub jobs tags."""
    if scenario == 'reset-loading':
        remove_tags = list(remove_tags) + CONSUMED_TAGS

    if scenario == 'consume':
        add_tags = list(add_tags) + CONSUMED_TAGS

    for job in filter(has_scraped_data, fetch_jobs(**opts)):
        # `click` fills tuples but scrapinghub expects lists
        shub.update_tags(job, add=list(add_tags), remove=list(remove_tags))
