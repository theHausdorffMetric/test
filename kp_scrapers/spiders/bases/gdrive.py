# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

from scrapy.spiders import Spider

from kp_scrapers.constants import BLANK_START_URL
from kp_scrapers.lib.services.gdrive import build_query, GDriveMimeTypes, GDriveService
from kp_scrapers.lib.services.shub import global_settings as Settings, validate_settings
from kp_scrapers.lib.xls import Workbook


TMP_DATA_DIR = '/tmp'
PROCESS_TAG = 'processed'


class GDriveSpider(Spider):
    start_urls = [BLANK_START_URL]

    def __init__(self, *args, **kwargs):
        super(GDriveSpider, self).__init__(*args, **kwargs)
        self.auto_parse = True
        # REQUIRED: specify base Gdrive dir in which to parse files
        self.path = kwargs['path']
        # REQUIRED: specify if subdirectories are to be parsed recursively too
        self.recursive = kwargs.get('recursive', '').lower() == 'true'
        # OPTIONAL: specify a file to be parsed regardless of consumed status
        self.file_to_process = kwargs.get('file_to_process')
        # OPTIONAL: bypass consumed status and force processing
        self.force_processing = kwargs.get('force', '').lower() == 'true'

        # init service
        self.service = GDriveService()

    @property
    def mime_types(self):
        """
        GDrive mime types of the files to retrieve
        Returns:
            list[str | unicode]
        """
        raise NotImplementedError()

    def parse_file_content(self, file_content):
        """
        Given the content of a file, parse it and yield resulting items.
        Args:
            file_content (str | unicode)
        Yields:
            dict
        """
        raise NotImplementedError()

    def parse(self, _):
        """
        Given the path to a single folder starting from the root, retrieves the files matching
        the filters and parses them.
        Each processed file is then tagged.
        Args:
            _: unused response argument
        Yields:
            dict
        """
        validate_settings('GOOGLE_DRIVE_BASE_FOLDER_ID')
        base_folder_id = Settings()['GOOGLE_DRIVE_BASE_FOLDER_ID']

        # check if we want to force parse one file only
        if self.file_to_process:
            self.logger.info(
                'Force "{}" to be parsed, even if its already processed'.format(
                    self.file_to_process
                )
            )
            query = build_query(mimes=self.mime_types, name=self.file_to_process)
        # if not, then parse as per normal
        else:
            query = build_query(
                mimes=self.mime_types,
                excluded_tags=[PROCESS_TAG] if not self.force_processing else [],
            )

        # find all files within specified path
        recursive = self.recursive and not self.file_to_process
        gfiles = self.service.list_files_in_path(
            base_folder_id, path=self.path, query=query, recursive=recursive
        )

        # parse each file found separately
        for gfile in gfiles:
            self.logger.info('Parsing gdrive file {}'.format(gfile))
            file_content = self.service.fetch_file_content(gfile)
            for item in self.parse_file_content(file_content):
                yield item
            self.service.tag_file(gfile['id'], [PROCESS_TAG])


class GDriveXlsSpider(GDriveSpider):
    def __init__(self, first_title=None, *args, **kwargs):
        """
        Args:
            first_title (str | unicode): name of the first element of title row
            *args:
            **kwargs:

        """
        super(GDriveXlsSpider, self).__init__(*args, **kwargs)
        self._first_title = first_title
        # OPTIONAL: specify if date is YYMMDD (False) or DDMMYY (True)
        self.day_first = kwargs.get('day_first', '').lower() == 'true'
        # OPTIONAL: specify if delimiters are present for multiple products, defaults to '/'
        self.delimiter = kwargs.get('delimiter', '/')

    @property
    def mime_types(self):
        return GDriveMimeTypes.SPREADSHEETS

    def parse_file_content(self, file_content):
        workbook = Workbook(content=file_content, first_title=self._first_title)
        for i, item in enumerate(workbook.items):
            try:
                for processed_item in self.process_item(item):
                    yield processed_item
            except Exception:
                self.logger.exception('Failed to parse item #{} ({})'.format(i + 1, item))

    def process_item(self, item):
        raise NotImplementedError()
