#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""Create and delete Scrapinghub projects."""

from __future__ import absolute_import, unicode_literals
import os
import sys

import click
import yaml

from kp_scrapers import vault  # noqa
from kp_scrapers.cli.project import default_project_name
from kp_scrapers.cli.ui import fail, info, success
from kp_scrapers.lib.services import shub


# special (permanent) project ids
SH_PRODUCTION_PROJECT = 321191
SH_PLAYGROUND_PROJECT = 6932
SH_LEGACY_PROJECT = 434

SH_KPLER_PROD_ORG = 288
SH_KPLER_TEST_ORG = 116804
# everything that is expected to be set for a new project on scrapinghub, whith
# default values when possible (i.e. no credentials set here, use the env, Luke)
PROJECT_SETTINGS = {
    'AUTOTHROTTLE_ENABLED': True,
    'AUTOTHROTTLE_START_DELAY': 5,
    # secret
    'AWS_ACCESS_KEY_ID': None,
    'AWS_SECRET_ACCESS_KEY': None,
    'DATADOG_API_KEY': None,
    'DATADOG_APP_KEY': None,
    'CRAWLERA_USER': None,
    'GOOGLE_DRIVE_BASE_FOLDER_ID': None,
    'GOOGLE_DRIVE_DEFAULT_USER': None,
    'GOOGLE_DRIVE_PRIVATE_KEY': None,
    'HISTORY_S3_BUCKET': None,
}


# TODO use click.CHOICES
@click.command()
@click.option(
    '-p', '--project', default='', help='project name, leave empty for a consistent experience'
)
@click.option('-C', '--copy-from', help='Copy settings from another project or local definition')
@click.option('--org', default=SH_KPLER_PROD_ORG, help='scrapinghub organization to use')
def create(project, copy_from, org):
    """Create a new project on Scrapinghub.

    One is expected to use default arguments so we can share the same
    development workflow.  Basically create a feature-branch, create a project,
    test on this project, merge and delete env.

    This wrapper uses by default the Kpler staging env to avoid eating
    production (paid) resources. Yet it also tries to reproduce as closely as
    possible this env, reading settings from env and pushing them on the new
    project.

    """
    project = project or default_project_name()

    info('creating project {} on {}'.format(project, org))
    res = shub.create_project(project, org)
    """Since it is not documented , here is `res.json()` format:
    {
        'info': {
            'description': ''
        },
        'owners': [],
        'ui_settings': {},
        'visual_project_type': '',
        'settings': {},
        'deleted': False,
        'description': '',
        'has_eggs': False,
        'monitoring_enabled': False,
        'organization_name': 'kpler.com',
        'last_activity': None,
        'version': None,
        'data_retention': 0,
        'organization': 288,
        'addons': [],
        'default_kumo_units': None,
        'id': 169023,
        'csv_fields': None,
        'name': 'testing-tmp3'
    }
    """
    if not res.ok:
        fail('failed to create project: {}'.format(res.text))
        sys.exit(1)

    feedback = res.json()
    success('project {} successfully created ({})'.format(project, feedback['id']))
    with shub.Config() as conf:
        info('updating config: {}'.format(conf.path))
        conf['projects'][project] = feedback['id']
        conf.commit()

    if copy_from:
        if copy_from.endswith('yml'):
            with open(copy_from, 'r') as fd:
                project_settings = {}
                for k, v in yaml.load(fd).items():
                    if isinstance(v, dict):
                        # the only case is when using `secret: 'vault.decrypt("....")'`
                        project_settings[k] = eval(v['secret'])
                    else:
                        project_settings[k] = v
        else:
            copy_from = shub.to_project_id(None, None, copy_from)
            info("populating project settings from project {}".format(copy_from))
            # TODO could be safer
            project_settings = shub.project_settings(copy_from).json().get('settings')
    else:
        info("populating project settings from env")
        project_settings = {}
        for k, v in PROJECT_SETTINGS.items():
            from_env = os.getenv(k)
            if from_env is None and v is None:
                info('no value defined for setting `{}`'.format(k))
            elif from_env:
                info('overwriting setting with env value: {}={}'.format(k, from_env))
                project_settings[k] = from_env

    res = shub.update_project_settings(feedback['id'], project_settings)
    if res.ok:
        success('successfully updated project settings')
    else:
        fail('failed to update project settings: {}'.format(res.text))


def protect_projects(ctx, param, value):
    if value and int(value) in [SH_PLAYGROUND_PROJECT, SH_PRODUCTION_PROJECT, SH_LEGACY_PROJECT]:
        fail(f"deleting project #{value} is forbidden")
        ctx.exit()


# TODO factorize project argument
@click.command()
@click.option(
    '-p',
    '--project',
    default='',
    callback=protect_projects,
    help='create a new project on ScrapingHub',
)
def delete(project):
    """Remove project from scrapinghub org.

    It is expected to be used with default arguments so it can find the same
    project created by by `create` command. This way one can use them at the
    start and the end of a feature-branch with namings already set for him.

    """
    project = project or default_project_name()

    with shub.Config() as conf:
        # crash when not found...
        project_id = conf['projects'].pop(project)

        res = shub.delete_project(project_id)
        if res.ok:
            success('project {} successfully deleted ({})'.format(project, project_id))
            # overwrite content on success
            conf.commit()
        else:
            fail('failed to delete project: {}'.format(res.text))
