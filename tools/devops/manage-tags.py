#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, unicode_literals

import click
from datadog import api, initialize

from kp_scrapers.commands import run_scrapy_command


click.disable_unicode_literals_warning = True

ROLE = 'scraping'
G_SCRAPER_TAGS = ['env:scrapinghub', f'role:{ROLE}']


@click.group()
@click.option('--api-key')
@click.option('--app-key')
def cli(api_key, app_key):
    initialize(api_key=api_key, app_key=app_key)


@cli.command()
@click.option('--project-id', default='321191')
def update(project_id):
    for metas in run_scrapy_command('describe', '--filter enabled:true --silent'):
        tags = ['category:' + metas['category']] + G_SCRAPER_TAGS
        hostname = '-'.join([metas['name'].lower(), project_id, ROLE])

        click.secho('creating {} tags: {}'.format(hostname, tags))
        api.Tag.create(hostname, tags=tags)


@cli.command()
@click.option('--pattern', default=ROLE)
def show(pattern):
    # Get tags by host id
    hosts = api.Infrastructure.search(q='hosts:{}'.format(pattern))
    for host in hosts['results']['hosts']:
        tags = api.Tag.get(host)
        print('{}: {}'.format(host, ', '.join(tags['tags'])))


if __name__ == '__main__':
    cli(auto_envvar_prefix='DATADOG')
