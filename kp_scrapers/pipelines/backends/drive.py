# -*- coding: utf-8 -*-

"""Google Drive custom storage backend.

See documentation in:
https://doc.scrapy.org/en/stable/topics/feed-exports.html?highlight=feed%20storage#storages

"""

from __future__ import absolute_import, unicode_literals
import codecs
import csv
import datetime as dt
from io import BytesIO
import logging

from kp_scrapers.lib.services.gdrive import GSheetsService


logger = logging.getLogger(__name__)


class DriveBackend(object):
    """Custom feed storage for exporting data to a Google Sheet.

    This class has been written to be consistent with the `FileFeedStorage` class in
    `scrapy.extensions.feedexport`.

    """

    FOLDER_MIMETYPE = 'application/vnd.google-apps.folder'
    SHEET_MIMETYPE = 'application/vnd.google-apps.spreadsheet'

    def __init__(self, parent_id, spider):
        """Initialize Google Drive storage with the raw export folder.

        Like on S3, raw exports are to be stored in a master "bucket" such as
        `kp-datalake`, with each spider receiving its own subfolder to stores
        their respective jobs.

        Args:
            dir_id (str): master folder ID
            spider (scrapy.Spider):

        """
        self.parent_id = parent_id
        self.spider = spider
        self.drive = GSheetsService()

    @property
    def sheet_name(self):
        """Generate a consistent, dynamic, sheet name."""
        return '{} {} - {}'.format(
            self.spider.name,
            self.spider.job_id,
            # remove milliseconds as it hinders readability
            dt.datetime.utcnow().isoformat()[:-7],
        )

    def open(self, spider):
        """Open Drive storage for the given spider.

        Like `kp-datalake` on S3, each spider receives its own subdir storage for raw export.
        The subdir might already be present inside the master dir.
        If so, use the existing one.

        Args:
            spider (scrapy.Spider):

        Returns:
            io.BytesIO:

        """
        self.spider_dir = self._get_spider_dir(spider)
        if not self.spider_dir:
            self.spider_dir = self.drive.create(
                name=self.spider.name, mimetype=self.FOLDER_MIMETYPE, parents=[self.parent_id]
            )

        # return file object to maintain API consisitency with rest of feed exporting framework
        # NOTE `CsvItemExporter` expects a binary file object, see:
        # https://doc.scrapy.org/en/latest/topics/exporters.html#scrapy.exporters.CsvItemExporter
        return BytesIO()

    def store(self, file_object, sheet_id=None):
        """Store the given file stream in a Google Sheet (CSV-formatted)

        Google's API does not support conversion into a Google Sheet with a simple
        file upload, even with the appropriate Google-speciifc MIME type.
        As an alternative, we create a file first, then modify the Sheet to export data.

        Args:
            file_object (io.BytesIO): file object containing exported data

        Returns:
            str | None: Google Sheet URL string if raw export is successful

        """
        # check if filestream is not an empty string
        file_object.seek(0)
        # `csv.reader` only supports unicodes, but `file_object` returns bytes
        file_data = list(csv.reader(codecs.iterdecode(file_object, 'utf-8')))
        file_object.close()
        if not file_data:
            logger.warning('Job returned no items scraped, Drive raw export will not proceed')
            return

        if not sheet_id:
            logger.info("no sheet provided, creating a new one")
            sheet = self.drive.create(
                name=self.sheet_name, mimetype=self.SHEET_MIMETYPE, parents=[self.spider_dir['id']]
            )
            sheet_id = sheet['id']
        else:
            logger.info("overwritting given sheet id={}".format(sheet_id))
            self.drive.clear_sheet(sheet_id)

        self.drive.write_sheet(file_id=sheet_id, rows=file_data)

        logger.info('spreadsheet ready: {}'.format(sheet_id))
        # expose url that might be handy for integrating with other middlewares
        return sheet_id

    def _get_spider_dir(self, spider):
        """Get metadata dictionary of spider subdir in master folder.

        Args:
            spider (scrapy.Spider):

        Returns:
            Dict[str, str] | None: return dict if dir exists, else None

        """
        for key in self.drive.list_children(self.parent_id):
            if key['name'] == spider.name and key['mimeType'] == self.FOLDER_MIMETYPE:
                return key
