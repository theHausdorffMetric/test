# -*- coding: utf-8 -*-

import functools
import os

import click
import dateutil.parser
import yaml

from kp_scrapers.cli.ui import fail, info
from kp_scrapers.lib.services import shub


def search_opts(func):
    @click.option(
        '-p',
        '--project',
        'project_id',
        default='production',
        callback=shub.to_project_id,
        help='Scrapinghub project to operate on',
    )
    @click.option('-W', '--with-tag', 'with_tags', multiple=True, help="only process those tags")
    @click.option('-S', '--skip-tag', 'skip_tags', multiple=True, help="dont process those tags")
    @click.option('-A', '--with-arg', 'with_args', multiple=True, help="only process those args")
    @click.option(
        '-J', '--job-key', 'job_keys', multiple=True, help='target jobs instead of search'
    )
    @click.option('-M', '--max-jobs', default=None, type=int, help='limit n of jobs to get')
    @click.option('--start-date', help="odler job limit to process")
    @click.option('--end-date', help="newer job limit to process")
    @click.argument('spider')
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def fetch_jobs(
    project_id, spider, with_tags, skip_tags, with_args, max_jobs, start_date, end_date, job_keys
):
    """Basically remap scrapinghub expectations with Kpler cli conventions."""
    search_opts = {'spider': spider, 'job_keys': job_keys}
    if max_jobs:
        search_opts['count'] = max_jobs
    if with_tags:
        search_opts['has_tag'] = list(with_tags)
    if skip_tags:
        search_opts['lacks_tag'] = list(skip_tags)
    if start_date:
        # shub client expects unix millisecond timestamp
        search_opts['startts'] = int(dateutil.parser.parse(start_date).strftime('%s')) * 1000
    if end_date:
        search_opts['endts'] = int(dateutil.parser.parse(end_date).strftime('%s')) * 1000

    info("fetching jobs project={}".format(project_id))
    for job in shub.spider_jobs(project_id, **search_opts):
        info("inspecting new job {}".format(job.key))
        # reformat like cli args
        # NOTE `job.metadata` is an object and its `get` method does not
        #       allow usage like so `.get('spider_args', {})`
        job_args = [f'{k}={v}' for k, v in (job.metadata.get('spider_args') or {}).items()]
        if not all([t in job_args for t in with_args]):
            info("skipping job {} with args: {}".format(job.key, job_args))
            continue

        yield job


def has_scraped_data(job):
    stats = job.metadata.get('scrapystats')
    if stats is None:
        fail('no stats available, skipping useless job {}'.format(job.key))
        return False

    count = stats.get('item_scraped_count')
    # TODO check why count is sometimes None
    if not count:
        fail("jobs didn't yield any data")
        return False

    return True


def _walk_files(root_path, filetype):
    return [
        os.sep.join([directory, filename])
        for (directory, dirs, files) in os.walk(root_path)
        for filename in files
        if filename[-len(filetype) :] == filetype
    ]


def walk_configs(conf_root=None, paths=None, blacklist=None):
    assert conf_root or paths

    blacklist = blacklist or []
    list_confs = paths or _walk_files(conf_root, filetype='yml')

    for conf_file in list_confs:
        if any(to_avoid in conf_file for to_avoid in blacklist):
            continue

        with open(conf_file, 'r') as fd:
            info(f'loading jobs in {conf_file}')
            yield yaml.load(fd)
