# -*- coding: utf-8 -*-

"""Helpers for working with kp-scrapers."""

from __future__ import absolute_import, unicode_literals
import subprocess


GIT_BRANCH_STYLE = {'long': '', 'short': '--short', 'human': '--abbrev-ref'}


def git_head_branch(style='human'):
    git_cmd = 'git rev-parse {style} HEAD'.format(style=GIT_BRANCH_STYLE.get(style, ''))
    return subprocess.check_output(git_cmd.split()).strip().decode()


def git_last_commit_msg():
    # git_cmd = 'git log -1 --pretty=%B'
    git_cmd = 'git log -1 --oneline'
    return subprocess.check_output(git_cmd.split()).strip().decode()


def git_last_tag():
    cmd = 'git describe --abbrev=0 --tags'
    return subprocess.check_output(cmd.split()).strip().decode()


def sanitize_project_fmt(raw):
    """Slugify human project name.

    Examples:
        >>> sanitize_project_fmt('foo/bar')
        'foo-bar'

    """
    return raw.encode('ascii', 'ignore').decode().replace('/', '-')


def default_project_name(prefix='kp-xxxx'):
    """Consistent default for project names based on git.

    Args:
        prefix(str): namespace - etl machines like  convention by default

    """
    project = sanitize_project_fmt(git_head_branch())
    return '.'.join([prefix, project])
