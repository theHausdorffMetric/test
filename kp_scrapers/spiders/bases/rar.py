import os

import rarfile
import requests
from scrapy.exceptions import CloseSpider
from scrapy.spiders import Spider


TMP_DATA_DIR = '/tmp'


class RarSpider(Spider):
    def __init__(self):
        self.data_path = os.path.join(os.getcwd(), TMP_DATA_DIR)
        if not os.access(self.data_path, os.R_OK):
            self.logger.warning(
                'SPIDER DISABLED: this spider requires a '
                'writable TMP_DATA_DIR (`{}` is not)'.format(self.data_path)
            )
            raise CloseSpider('`{}` has to be a writable directory'.format(self.data_path))

    def save_file(self, filename, content):
        """Save the rar file scraped on disk.

        Writing the file on disk is required, so that an external PDF to Text
        conversion tool can access it.

        Returns:
            bool: False, if a file with the same name already exists in WORKDIR/tmp
                  Otherwise save the file in WORKDIR/tmp and return True

        """
        data_path = os.path.join(self.data_path, filename)

        # even if file exists, it will be overwritten
        open(data_path, 'wb').write(content)
        self.logger.info('Saved new file %s', filename)

    def delete_rar_files(self):
        """Delete rar files downloaded on disk
        """
        for root, dirs, files in os.walk(self.data_path):
            for filename in files:
                if filename.endswith('.rar'):
                    os.remove(os.path.join(root, filename))

    def extract_rar_io(self, rar_urls):
        """Extract documents in a given rar archive.

        Args:
            rar_urls (list[str]): urls of rar part files forming the rar archive

        Returns:
            list[str]: Result in a list of paths of documents in the rar archive

        """

        # download each part of the rar archive
        for url in rar_urls:
            filename = url.split('/')[-1]
            self.save_file(filename, requests.get(url).content)
            # save path of the first rar file since
            # if you open first rar with RarFile,
            # you get all files from all volumes.
            if filename.endswith('part01.rar'):
                main_filename = filename

        # open rar file
        open_rar = rarfile.RarFile(os.path.join(self.data_path, main_filename))
        # extract documents in the rar file and return their path
        documents = []
        for document_name in open_rar.namelist():
            open_rar.extract(document_name, self.data_path)
            document_path = os.path.join(self.data_path, document_name)
            documents.append(document_path)

        # delete downloaded rar files
        self.delete_rar_files()

        return documents
