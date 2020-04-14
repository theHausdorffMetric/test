# -*- coding: utf-8 -*-

"""Scraping Hub interfaces."""

from __future__ import absolute_import
from collections import namedtuple
import json
import logging
import os

import requests
from requests.auth import HTTPBasicAuth
from scrapinghub import ScrapinghubClient
from scrapy.utils.project import get_project_settings as Settings
import yaml

from kp_scrapers.cli import ui
from kp_scrapers.lib import utils
from kp_scrapers.lib.errors import InvalidSettings
from kp_scrapers.lib.utils import retry


SH_API_VERSION = 'v2'
SH_API_BASE = 'https://app.scrapinghub.com/api/{v}/{resrc}'
SH_API_KEY = os.getenv('SH_API_KEY')
# SH convention when choosing between project types Scrapy and Portia
SCRAPY_PROJECT = ''
HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, sdch, br',
    'Connection': 'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like'
    ' Gecko) Chrome/58.0.3029.110 Safari/537.36',
}
SPIDER_DONE_STATE = 'finished'

# Scrapinghub uses a slightly different format to define periodic jobs than cron
SHCronType = namedtuple('SHCronType', ['month', 'day_of_month', 'day_of_week', 'hour', 'min'])

logger = logging.getLogger(__name__)

# NOTE for now this is the only valid collection we have. It would be
# interesting though to clone it (or fake it) on other environments so that
# testing/staging envs don't interact with the produciton data
# Then we will be able to replace the constant with a value read from env (dev)
# or settings (scrapinghub)
# NOTE read that from scrapinghub.yml ?
# know that.
PROD_PROJECT_ID = 321191


def global_settings(namespace='project_settings'):
    """Unify Scrapy and Scrapinghub settings into one entrypoint.

    See the following for namespaces of Scrapinghub settings:
    http://shub.readthedocs.io/en/stable/custom-images-contract.html#shub-settings

    As a side effect, also try to make it work locally in order to have a smooth experience
    going back and forth between development and playground/production.

    NOTE 1: It turns out that shub doubly escapes settings before putting them
    into env['SHUB_SETTINGS'] It means that '\n' in a settings will be stored
    as '\\\\n' in 'SHUB_SETTINGS'.  json.loads does a single unescape, so we
    need to unescape a second time.

    NOTE 2: A good way of not having to update every file each time we change
    how we import settings is to use it this way:

            from kp_scrapers.lib.services.shub import global_settings as Settings

    """
    raw_shub_settings = (
        os.environ.get('SHUB_SETTINGS', '{}').encode('utf-8').decode('unicode_escape')
    )
    shub_settings = json.loads(raw_shub_settings).get(namespace)
    scrapy_settings = Settings().copy()
    # merge them
    scrapy_settings.update(shub_settings)

    return scrapy_settings


def shub_conn():
    # don't use default `.get()` property because then it will evaluate
    # `settings.SH_API_KEY` anyway and you might not have setup it locally
    api_key = os.environ.get('SH_API_KEY') or Settings().get('SH_API_KEY')

    # NOTE not really safe when `name` doesn't exist
    return ScrapinghubClient(api_key)


def validate_settings(*mandatory_fields, **kwargs):
    """Crash on missing settings with a proper exception."""
    # initial mean of the function is to validate Scrapy settings and it acts
    # as such by default. But we keep the door open to validate other
    # configuration container (like `os.environ`)
    settings = kwargs.get('settings') or global_settings()

    if not all(k in settings for k in mandatory_fields):
        raise InvalidSettings(*mandatory_fields)


@utils.deprecated('scrapinghub collection are no longer used')
def fetch_shub_collection(name, project_id=PROD_PROJECT_ID):
    """Import Scrapinghub collection as list.

    Args:
        name (str): Scrapinghub collection name
        project_id (int): unique environement ID on Scrapinghub. You can find
                          it in the url when you work in this env.
                          Currently only prod collection exists.

    Returns:
        list: items stored in on Scrapinghub

    """
    logger.info('initializing "{}" collection data'.format(name))
    # NOTE not really safe when `name` doesn't exist
    return shub_conn().projects.get(project_id).collections.get_store(name).list()


def project_settings(project_id):
    resrc = 'projects/{}'.format(project_id)
    return requests.get(
        SH_API_BASE.format(v=SH_API_VERSION, resrc=resrc),
        auth=HTTPBasicAuth(SH_API_KEY, ''),
        headers=HEADERS,
    )


def periodic_jobs(project_id):
    resrc = 'projects/{}/periodicjobs'.format(project_id)
    return requests.get(
        SH_API_BASE.format(v=SH_API_VERSION, resrc=resrc),
        auth=HTTPBasicAuth(SH_API_KEY, ''),
        headers=HEADERS,
    )


def delete_periodic_job(project_id, job_id):
    resrc = 'projects/{}/periodicjobs/{}'.format(project_id, job_id)
    return requests.delete(
        SH_API_BASE.format(v=SH_API_VERSION, resrc=resrc),
        auth=HTTPBasicAuth(SH_API_KEY, ''),
        headers=HEADERS,
    )


def create_project(name, organization):
    payload = {
        'organization': organization,
        'name': name,
        'visual_project_type': SCRAPY_PROJECT,  # optional (and useless)
    }

    return requests.post(
        SH_API_BASE.format(v=SH_API_VERSION, resrc='projects'),
        auth=HTTPBasicAuth(SH_API_KEY, ''),
        json=payload,
    )


def update_project_settings(project_id, settings):
    payload = {'settings': settings}
    url = SH_API_BASE.format(v=SH_API_VERSION, resrc='projects/{}'.format(project_id))
    return requests.patch(url, auth=HTTPBasicAuth(SH_API_KEY, ''), json=payload)


def update_spider_settings(project_id, spider_id, settings):
    # NOTE this function will completely erase existing settings and overwrite with new ones
    payload = {'settings': settings}
    url = SH_API_BASE.format(v=SH_API_VERSION, resrc=f'projects/{project_id}/spiders/{spider_id}')
    return requests.patch(url, auth=HTTPBasicAuth(SH_API_KEY, ''), json=payload)


def delete_project(project_id):
    url = SH_API_BASE.format(v=SH_API_VERSION, resrc='projects/{}'.format(project_id))
    return requests.delete(url, auth=HTTPBasicAuth(SH_API_KEY, ''), json={})


def reset_periodic_jobs(project_id, job_names, dry_run=False):
    res = periodic_jobs(project_id)
    if res.status_code != 200:
        raise ValueError('failed to retrieve jobs list: {}'.format(res.text))

    for job in res.json().get('results', []):
        spiders = [s['name'] for s in job['spiders']]
        if set(job_names).intersection(spiders) and job['type'] == 'spider':
            ui.info('existing job found ({}), reseting'.format(job['id']))
            if not dry_run:
                res = delete_periodic_job(project_id, job['id'])
                assert res.status_code == 204


def create_periodic_job(spider, project_id, cron, args, tags, description, priority, dry_run):
    # TODO support dry_run
    cron = SHCronType(*cron.replace(' ', '').split(','))
    payload = {
        'month': cron.month,
        'dayofmonth': cron.day_of_month,
        'day': cron.day_of_week,
        'hour': cron.hour,
        'minutes_shift': cron.min,
        "spiders": [{'name': spider, 'spider_args': args, 'priority': priority}],
        'description': description,
        'addtags': tags,
    }

    if dry_run:
        ui.info('job schedule: {}'.format(payload))
        # fake job id, no error
        return 222, None

    resrc = 'projects/{}/periodicjobs'.format(project_id)
    url = SH_API_BASE.format(v=SH_API_VERSION, resrc=resrc)
    # NOTE no headers ?
    res = requests.post(url, auth=HTTPBasicAuth(SH_API_KEY, ''), headers=HEADERS, json=payload)
    if res.status_code != 201:
        # TODO rollback the whole deployment ?
        return None, res.text

    return res.json().get('id'), None


class Config(dict):
    # as expected by default by scrapinghub
    default_filename = 'scrapinghub.yml'

    def __init__(self, autocommit=False):
        self.autocommit = autocommit

    # /// configure properties and helpers

    @property
    def path(self):
        # TODO make it smarter to handle calls outside root directory
        return os.path.join(os.getcwd(), self.default_filename)

    def commit(self):
        """Overwrite content with current config state."""
        # convert to yaml
        # force dict first as passing an object (self) yields a  `RepresentError`
        current_conf = yaml.safe_dump(dict(self), default_flow_style=False)

        # overwrite old content
        self._conf_fd.seek(0)
        self._conf_fd.write(current_conf)
        self._conf_fd.truncate()

    def to_project_id(self, given_env):
        """For UX sake prefer users to give env names on SHUB than remeber ids."""
        for env, env_id in self['projects'].items():
            if env == given_env:
                return env_id

        # if nothing found above, indicate it with a `None`
        return None

    # /// context manager behaviour

    def __enter__(self):
        self._conf_fd = open(self.path, 'r+')
        self.update(yaml.load(self._conf_fd))
        return self

    def __exit__(self, type_, value, traceback):
        # TODO if traceback
        if self.autocommit:
            self.commit()
        self._conf_fd.close()


# TODO implement stop based on date (job.metadata.get('start_time'))
def spider_jobs(project_id, spider, **opts):
    """Iterate over jobs that match the specified parameters.

    For a list of possible parameters, see:
    https://python-scrapinghub.readthedocs.io/en/latest/client/apidocs.html#scrapinghub.client.jobs.Jobs.iter

    Args:
        project_id (int | str): integer or string numeric project id
        spider (str): filter by spider name

    Returns:
        GeneratorType[Dict]:

    """
    shub_client = shub_conn()
    job_keys = opts.pop('job_keys', None)

    if not job_keys:
        params = {'spider': spider, 'state': SPIDER_DONE_STATE}
        params.update(opts)
        job_keys = shub_client.get_project(project_id).jobs.iter(**params)
    else:
        # fake scrapinghub format so the generator below don't notice
        job_keys = [{'key': job_key} for job_key in job_keys]

    return (shub_client.get_job(summary['key']) for summary in job_keys)


def to_spider_id(project_id, spider_name):
    """Given an env id and human spider name, map the related id for the SHUB api.
    """
    return shub_conn().get_project(project_id).spiders.get(spider_name)._id


def to_project_id(ctx, param, value):
    """Given a human env name, map the related id for the SHUB api."""
    with Config() as c:
        return c.to_project_id(value)


# NOTE could be provided by the job extension
def spider_job_url(job_full_id):
    return 'https://app.scrapinghub.com/p/{}'.format(job_full_id)


@retry(tries=3, wait=1)
def update_tags(job, **action):
    """wrapper aroung scrapinghub `job.update_tag`.

    This is necessary since shub returns an HTTP error occasionally for unknown reasons.

    Args:
        job (scrapinghub.client.jobs.Job):
        action (Dict): `<action>=tags` items to manipulate job tags

    """
    job.update_tags(**action)
