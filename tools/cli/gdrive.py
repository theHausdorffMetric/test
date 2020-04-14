#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""Programmatic interface for accessing Google Drive file metadata

As of Feb 2018, there is no native frontend for accessing a file's metadata.
Gdrive files that have been proccessed by spiders will forever remain tagged
as `processed`, making it impossible to re-scrape a file upon making changes.

This script provides an way to view Gdrive files, and modify metadata.

NOTE
~~~~
    - gdrive credentials are required, either define them in `local_setting,py` or
      export them as environment variables

TODO
~~~~

    - suggestions welcome

"""

from __future__ import absolute_import, unicode_literals

import click

from kp_scrapers.lib.services.gdrive import build_query, GDriveService
from kp_scrapers.lib.services.shub import global_settings as Settings, validate_settings


gdrive = GDriveService()

validate_settings('GOOGLE_DRIVE_BASE_FOLDER_ID')
BASE_FOLDER_ID = Settings()['GOOGLE_DRIVE_BASE_FOLDER_ID']

DEFAULT_FILE_FIELDS = 'parents, mimeType, id, name'  # noqa
PROCESS_TAG = 'processed'  # noqa
CPP_TEST = 'Reports/Ship Agents/Generic'  # noqa, to be removed once script is fully tested


@click.group()
def cli():
    pass


def set_files(path, name, tags, mime_types, do_add_tag):
    """Sets the tags of the specified file to the provided value

        Args:
            path (str | unicode): relative file path from base folder as defined in settings
            name (str | unicode)
            tags (list[(str | unicode)])
            mime_types (list[(str | unicode)])
            do_add_tag (str | unicode | None): value of property field to put in file metadata

    """
    files = gdrive.list_files_in_path(
        BASE_FOLDER_ID, path=path, query=build_query(mimes=mime_types, name=name)
    )

    click.confirm(
        '{} files were found\n> {}\nDo you want to continue?\n'.format(
            len(files), '\n> '.join([f['name'] for f in files])
        ),
        abort=True,
    )

    for file in files:
        if file['name'] in name:
            if do_add_tag:
                gdrive.tag_file(file['id'], tags)
                click.secho('Tagged GDrive file {} as {}'.format(file['name'], tags))
            else:
                gdrive.untag_file(file['id'], tags)
                click.secho('Untagged GDrive file {} by removing {}'.format(file['name'], tags))


@cli.command(help='Tag specified Google Drive files as processed')
@click.option('-p', '--path', required=True, help='Path where tag reside in')
@click.option(
    '-f', '--file', required=True, multiple=True, help='Case-insensitive name of gdrive file to tag'
)
@click.option('-t', '--tag', required=True, multiple=True, help='Tag to apply to gdrive file')
@click.option('-m', '--mime-type', multiple=True, help='MIME type to list')
def tag_files(path, names, tags, mime_types):
    # `do_add_tag` convention following GDriveService's methods' parameters
    set_files(path, names, tags, mime_types, do_add_tag='True')


@cli.command(help='Untag specified, processed Google Drive files')
@click.option('-p', '--path', required=True, help='Path where tag reside in')
@click.option(
    '-f', '--file', required=True, multiple=True, help='Case-insensitive name of gdrive file to tag'
)
@click.option('-t', '--tag', required=True, multiple=True, help='Tag to apply to gdrive file')
@click.option('-m', '--mime-type', multiple=True, help='MIME type to list')
def untag_files(path, names, tags, mime_types):
    # `do_add_tag` convention following GDriveService's methods' parameters
    set_files(path, names, tags, mime_types, do_add_tag=None)


# def list_files(type_=None, name=None, mimes=None, query=None, page_size=1000):
@cli.command(help='List files in path')
@click.option('-d', '--type', help='Type of the files, FILE or FOLDER (use GDriveFileType)')
@click.option(
    '-f', '--file', required=True, multiple=True, help='Case-insensitive name of gdrive file to tag'
)
@click.option(
    '-f', '--file', required=True, multiple=True, help='Case-insensitive name of gdrive file to tag'
)
def find_tagged_files(names, mimes):
    """Wrapper for finding all files matching the filters provided

        Args:
            type_ (str | unicode): Type of the files, FILE or FOLDER (use GDriveFileType)
            name (str | unicode): Name of the files
            query (str | unicode): Additional query to use to filter the files
            page_size (int): limit of files par page/call

    """
    # TODO finish this function
    pass


if __name__ == '__main__':
    cli()
