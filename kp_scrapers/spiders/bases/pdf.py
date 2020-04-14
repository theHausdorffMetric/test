# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import codecs
import csv
from hashlib import md5
import logging
import os
import signal
from subprocess import PIPE, Popen

from scrapy.exceptions import CloseSpider
from scrapy.spiders import Spider
import six

from kp_scrapers.lib.services.shub import global_settings as Settings, validate_settings


TMP_DATA_DIR = '/tmp'


class TimeoutException(Exception):
    pass


def signal_callback(signal, frame):
    raise TimeoutException()


def tabula_command(dir_path, filename, jar_file_path, **kwargs):
    """Build tabula.jar shell command as a list.

    Args:
        dir_path (str): absolute path of output dir
        filename (str): name of output file
        jar_file_path (str): absolute path of `tabula.jar`

    Returns:
        commands (list[str]):

    """
    commands = [
        'java',
        '-Dfile.encoding=utf-8',
        '-Xms256M',
        '-Xmx1024M',
        '-jar',
        jar_file_path,
        '--outfile',
        os.path.join(dir_path, filename + '.csv'),
        os.path.join(dir_path, filename + '.pdf'),
    ]

    for key, value in six.iteritems(kwargs):
        # insert addtional command-line flag for jvm
        commands.append(key)
        # insert optional arguments for above flag
        commands.extend(value)

    return commands


class PdfSpider(Spider):
    def __init__(self, *args, **kwargs):
        super(PdfSpider, self).__init__(*args, **kwargs)
        # Having auto_parse set to True by default is very useful for testing
        self.auto_parse = kwargs.get('auto_parse', True)
        self.data_path = os.path.join(os.getcwd(), TMP_DATA_DIR)
        if not os.access(self.data_path, os.R_OK):
            self.logger.warning(
                'SPIDER DISABLED: this spider requires a '
                'writable TMP_DATA_DIR (`{}` is not)'.format(self.data_path)
            )
            raise CloseSpider('`{}` has to be a writable directory'.format(self.data_path))

    def save_file(self, filename, content):
        """Saves the PDF scraped on disk.

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

    def generate_filename(self, body):
        """Generate a unique filename with the spider file name and the Request's fingerprint.

        Args:
            body (str): filestream

        Returns:
            str: the filename

        """
        # cutoff at 10000 characters to avoid possible errors due to large files
        return md5(body[:10000]).hexdigest()

    def _fork_tabula(self, body, **kwargs):
        """Wrapping of interactions with file system and tabula in a function.

        The key of each entry in the kwargs is the option for tabula and the value
        should be a list of strings for the values.

        If the kwargs is {'-p': ['all']}, the following will be executed:
            java ... -p all
        To set a command-line flag with no argument, set the value as empty list:
            {'-l': [], ...}

        Args:
            body (str): filestream
            **kwargs: The options for tabula.

        Returns:
            list[list[str]]: Result in list of lists from tabula.

        """
        # obtain tabula path
        validate_settings('TABULA_JAR_PATH')
        jar_file_path = Settings()['TABULA_JAR_PATH']
        if not os.path.isfile(jar_file_path):
            raise IOError(
                'Tabula.jar not found at `{}`, check `local_settings.py`'.format(jar_file_path)
            )
        self.logger.debug('Using tabula.jar at `{}`'.format(jar_file_path))
        filename = self.generate_filename(body)
        self.save_file(filename + '.pdf', body)
        tabula_subprocess = Popen(
            tabula_command(self.data_path, filename, jar_file_path, **kwargs),
            stdout=PIPE,
            stderr=PIPE,
        )
        signal.alarm(300)
        output, error = tabula_subprocess.communicate()

        if error:
            # warnings regarding missing fonts by the java process are safe to ignore
            # warnings regarding end of stream may be ignored, however please check output integrity
            self.logger.warning(
                'Tabula.jar has encountered warnings/exceptions while processing, '
                'please verify output integrity\n{}'.format(error.decode('utf-8'))
            )

    def extract_pdf_table(
        self, response, information_parser=lambda *args: args, use_dict_reader=False, **kwargs
    ):
        """Parse PDF given a scrapy Response.

        This method is merely a wrapper around the existing
        PDF extractor method `self.extract_pdf_io()`.

        Args:
            response (scrapy.Response):
            information_parser: Iterator to filter unwanted rows, defaults to an identity function
            use_dict_reader(boolean): use DictReader if True, other if False
            **kwargs: additional tabula options to for fine-tuning parsing

        Returns:
            list[list[str]]: Result in list of lists from tabula

        """
        return self.extract_pdf_io(response.body, information_parser, use_dict_reader, **kwargs)

    def extract_pdf_io(self, body, preprocessor=None, use_dict_reader=False, **kwargs):
        """Parse pdf given the PDF's filestream.

        Args:
            body (str): PDF filestream
            preprocessor (callable): Iterator to filter unwanted rows, defaults to an identity fn
            use_dict_reader(boolean): use DictReader if True, other if False
            **kwargs: additional tabula options for fine-tuning options

        Returns:
            list[list[str]]: Result in list of lists from tabula

        """
        self._fork_tabula(body, **kwargs)
        filename = self.generate_filename(body)

        with open(os.path.join(self.data_path, filename + '.csv'), 'r') as csvfile:
            # Used 'DictReader' to better parse USCustoms table
            # we may work only with it in the future and get rid of 'reader'
            if not use_dict_reader:
                table = csv.reader(csvfile)

                def _identity(_):
                    return _

                preprocessor = preprocessor or _identity
                information = [info for info in preprocessor(table)]
            else:
                information = list(csv.DictReader(csvfile))

            os.remove(os.path.join(self.data_path, filename + '.pdf'))
            os.remove(os.path.join(self.data_path, filename + '.csv'))

        return information

    @classmethod
    def pdf_to_text(cls, filepath, encoding='utf-8', page=None):
        """Returns a text representation of the pdf.

        Requires poppler's pdftotext installed on system.

        Returns:
            str:

        """
        signal.signal(signal.SIGALRM, signal_callback)

        if not page:
            command = ['pdftotext', filepath, '-layout', '-']
        else:
            page = str(page)
            command = ['pdftotext', filepath, '-layout', '-f', page, '-l', page, '-']

        p = Popen(command, stdin=PIPE, stdout=PIPE, stderr=PIPE)

        signal.alarm(60)
        try:
            output, error = p.communicate()
            # Disable the alarm, signal_callback won't be called
            signal.alarm(0)
        except TimeoutException:
            error = 'pdftotext taking too much time to process, terminating process'
            cls.get_logger().error(error)
            p.terminate()
            raise RuntimeError(error)

        output = codecs.decode(output, encoding)

        return output

    @classmethod
    def get_logger(cls):
        return logging.getLogger(cls.name)
