#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""Manage scrapinghub periodic jobs."""

from __future__ import absolute_import, unicode_literals
import functools
import itertools
import os
from random import randrange
import sys
import time

import click
import datadog

from kp_scrapers import vault  # noqa
from kp_scrapers.cli.ui import fail, info, success
from kp_scrapers.cli.utils import walk_configs
from kp_scrapers.lib.services import shub


import datetime as dt  # noqa; used for dynamically generating `max_year` for Equasis schedules


DEFAULT_PRIORITY = 2
DEFAULT_DESCRIPTION = "Add custom description in the scheduling files"

GLOBAL_JOB_TAGS = ["auto-schedule"]


def render_args(tpl):
    def _force_list(may_list):
        return may_list if isinstance(may_list, list) else [may_list]

    individuals = [[{name: arg} for arg in _force_list(eval(code))] for name, code in tpl.items()]

    return [
        {k: v for d in option for k, v in d.items()} for option in itertools.product(*individuals)
    ]


def generate_cron(template):
    return template.format(
        month=randrange(1, 13),
        dayofmonth=randrange(1, 31),
        dayofweek=randrange(1, 8),
        hour=randrange(0, 24),
        minute=randrange(0, 60),
    )


def run_opts(func):
    @click.option(
        "-p",
        "--project",
        "project_id",
        default="production",
        callback=shub.to_project_id,
        help="ScrapingHub environment",
    )
    @click.option("-c", "--config", "conf_files", multiple=True, default=None)
    @click.option("-R", "--root-conf", default=None)
    @click.option("-s", "--spider", "spiders", multiple=True, default=None)
    @click.option("--dry-run", is_flag=True)
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


@click.command("scrape")
@run_opts
@click.option("-t", "--tail", is_flag=True, default=False, help="tail spider logs")
@click.option('-a', '--args', multiple=True, help='NAME=VALUE')
def scrape(project_id, spiders, root_conf, conf_files, tail, dry_run, args):
    """Run spiders on Scrapinghub based on configure files or command line argument"""
    spider_names = spiders or []

    if conf_files:
        # force yielding all the configs so wwe can merge spider names
        configs = list(
            walk_configs(conf_root=root_conf, paths=conf_files, blacklist=["settings.yml"])
        )

        if not spider_names:
            # do we really want to scrape all spiders if spider not specified?
            for conf in configs:
                spider_names.extend([job_spec["spider"] for job_spec in conf["jobs"]])

        for specs in configs:
            global_tags = GLOBAL_JOB_TAGS + specs.get("global_tags", [])

            for raw_job_spec in specs['jobs']:
                spider_name = raw_job_spec['spider']
                if raw_job_spec['spider'] not in spider_names:
                    info(f'spider {spider_name} not required to schedule')

                if raw_job_spec.get('disabled'):
                    info(f'spider {spider_name} disabled by configuration file')

                args = raw_job_spec.get('args', {})
                tags = global_tags + raw_job_spec.get('tags', [])
                priority = raw_job_spec.get('priority', None)

                # dynamic arguments
                for combination in render_args(raw_job_spec.get("dynamic_args", {})):
                    args.update(combination)

                    if not dry_run:
                        job_spec = _build_job_spec(spider_name, args, tags, priority)
                        _run_spider(project_id, job_spec, tail)
    else:
        if not spider_names:
            fail('Please specify spider name.')
            return

        job_args = {}
        for key_value in args:
            key, _, value = key_value.partition('=')
            job_args.update({key: value})

        if not dry_run:
            for spider in spider_names:
                job_spec = _build_job_spec(spider, args=job_args)
                _run_spider(project_id, job_spec, tail)


def _build_job_spec(name, args=None, tags=None, priority=None):
    job_spec = {'spider_name': name, 'priority': DEFAULT_PRIORITY}
    if args:
        job_spec.update(args=args)
    if tags:
        job_spec['tags'] = tags
    if priority:
        job_spec['priority'] = priority

    return job_spec


def _run_spider(project_id, job_spec, tail):
    """Connect to Scrapinghub and run the job."""
    spider = job_spec.get('spider_name')

    info(
        f'running {spider} on scrapinghub: {project_id}: '
        f'args={job_spec.get("args")} tags={job_spec.get("tags")}'
    )

    from kp_scrapers.lib.services.shub import shub_conn

    job = (
        shub_conn()
        .get_project(project_id)
        .jobs.run(
            spider,
            add_tag=job_spec.get('tags'),
            job_args=job_spec.get('args'),
            priority=job_spec.get('priority'),
        )
    )
    url = f'https://app.scrapinghub.com/p/{job.key}'
    success(f'{spider} is scraping at {url}')

    if tail:
        # wait for the job to start
        info("starting to tail logs...")
        while job.metadata.get("state") != "finished":
            time.sleep(5)
            for line in job.logs.list():
                info("[ {} ] {}".format(job_spec["spider"], line))
        sys.exit(0 if job.metadata.get("close_reason") == "finished" else 1)


@click.command("batch-retire")
@run_opts
def batch_retire(project_id, spiders, root_conf, conf_files, dry_run):
    """Register several periodic jobs from a yaml conf."""
    # force yielding all the configs so wwe can merge spider names
    spider_names = spiders or []
    if not spiders:
        configs = walk_configs(conf_root=root_conf, paths=conf_files, blacklist=["settings.yml"])
        for conf in configs:
            spider_names.extend([job_spec["spider"] for job_spec in conf["jobs"]])

    shub.reset_periodic_jobs(project_id, set(spider_names), dry_run)


@click.command("batch-schedule")
@run_opts
@click.option("--reset/--no-reset", default=True)
@click.option("--monitor/--no-monitor", default=False)
def batch_schedule(project_id, spiders, root_conf, conf_files, dry_run, reset, monitor):
    """Register several periodic jobs from a yaml conf."""
    datadog.initialize(api_key=os.getenv("DATADOG_API_KEY"), app_key=os.getenv("DATADOG_APP_KEY"))

    # force yielding all the configs so wwe can merge spider names
    configs = list(walk_configs(conf_root=root_conf, paths=conf_files, blacklist=["settings.yml"]))
    spider_names = spiders or []
    if not spiders:
        for conf in configs:
            spider_names.extend([job_spec["spider"] for job_spec in conf["jobs"]])

    if reset:
        # be gentle with the api and batch delete the jobs upfront
        shub.reset_periodic_jobs(project_id, set(spider_names), dry_run)

    # TODO validate configuration (a seperate command ?)
    for specs in configs:
        # init with global settings that apply for all
        g_tags = GLOBAL_JOB_TAGS + specs.get("global_tags", [])
        g_settings = specs.get('global_settings', {})
        d_crons = specs.get("default_crons", [])

        for job_spec in specs["jobs"]:
            # one can limit spiders from the command line
            if job_spec["spider"] not in spider_names:
                info('spider "{}" not required to schedule'.format(job_spec["spider"]))
                continue

            if job_spec.get("disabled"):
                info('spider "{}" disabled by config'.format(job_spec["spider"]))
                continue

            if not (job_spec.get("crons") or d_crons):
                fail('spider "{}" contains no cron'.format(job_spec["spider"]))
                continue

            # overwrite spider-level settings on scrapinghub
            if job_spec.get("settings") or g_settings:
                # use default settings if there exists any
                job_spec["settings"] = {**job_spec.get("settings", {}), **g_settings}
                for key, value in job_spec["settings"].items():
                    if isinstance(value, dict):
                        # the only case is when using `secret: 'vault.decrypt("....")'`
                        job_spec["settings"][key] = eval(value['secret'])

                spider_id = shub.to_spider_id(project_id, job_spec["spider"])
                res = shub.update_spider_settings(project_id, spider_id, job_spec["settings"])
                if not res.ok:
                    fail(f'failed to update settings for spider "{job_spec["spider"]}"')
                    # skip since we don't want scheduled jobs to fail due to incorrect settings
                    continue

            # fill defaults
            # NOTE propably better done merging a hierarchy of dicts
            job_spec["priority"] = job_spec.get("priority", DEFAULT_PRIORITY)
            job_spec["tags"] = g_tags + job_spec.get("tags", [])
            job_spec["crons"] = job_spec.get("crons") or d_crons

            for combination in render_args(job_spec.get("dynamic_args", {})):
                # add static arguments for every combination generated
                combination.update(job_spec.get("args", {}))

                for cron_tpl in job_spec["crons"]:
                    cron = generate_cron(cron_tpl)
                    info(f"creating job on project {project_id}: {cron}")
                    job_id, err = shub.create_periodic_job(
                        job_spec["spider"],
                        project_id,
                        cron,
                        combination,
                        job_spec["tags"],
                        job_spec.get("description", DEFAULT_DESCRIPTION),
                        job_spec["priority"],
                        dry_run,
                    )
                    if job_id or dry_run:
                        success(f"done: {job_id}")
                    else:
                        fail(f"failed to schedule job: {err}")

            if job_spec.get("monitoring") and monitor:
                job_spec["monitoring"]["tags"] = job_spec["monitoring"].get("tags", [])
                if "creator:bot" not in job_spec["monitoring"]["tags"]:
                    job_spec["monitoring"]["tags"].append("creator:bot")

                # TODO and only if the command above worked fine
                # Create a new monitor (don't care about the dynamic nature of arguments)
                try:
                    info("creating new datadog monitor: {}".format(job_spec["monitoring"]))
                    if not dry_run:
                        feedback = datadog.api.Monitor.create(
                            type="metric alert", **job_spec["monitoring"]
                        )
                        if feedback.get("errors"):
                            fail("failed to create monitor: {}".format(feedback["errors"]))
                        else:
                            success("successfully created alert on {}".format(feedback["created"]))
                except ValueError as e:
                    # usually error 403 forbidden that return an HTML page instead of json
                    fail("failed to create monitor: {}".format(e))


# TODO support click.Choice over envs
# TODO better description of cron like
# TODO support create/delete/update commands too based on resource (project, periodic jobs)
@click.command()
@click.option(
    "-p",
    "--project",
    "project_id",
    default="production",
    callback=shub.to_project_id,
    help="create a new project on ScrapingHub",
)
@click.option("-a", "--arg", "args", multiple=True, help="spider job argument")
@click.option("-t", "--tag", "tags", multiple=True)
@click.option("-c", "--cron", help="cron like shub schedule")
@click.option("-d", "--description", default="")
@click.option("-p", "--priority", default=2)
@click.argument("spider")
def schedule(**kwargs):
    """Register a single periodic job."""
    spider_args = {k: v for k, v in list(map(lambda x: x.split("="), kwargs["args"]))}
    kwargs["args"] = spider_args
    job_id, err = shub.create_periodic_job(**kwargs)
    if job_id is None:
        fail(f"failed to create job: {err}")
    else:
        success("successfully created periodic job (id={})".format(job_id))
