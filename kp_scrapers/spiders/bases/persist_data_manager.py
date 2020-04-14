# -*- coding: utf8 -*-

"""
Keep on SH memory for spiders, Useful to know last execution time

TODO:
    NCA: In my opinion this is an "implemetation detail" of the
    :class:`~kp_scrapers.spiders.bases.persistspider.PersistSpider` class and
    thus should not be exposed here. It should be a private class in that class
    module.

    It is important to think about what you should and should not
    expose when you design an API. Here the API should be as simple
    as::

        class MySpider(PersistSpider):

            def __init__(self, my_spider_specific_args=None, **kwargs):
                super(MySpider, self).__init__(**kwargs)

        spider = MySpider(state_filename='my-spider-state')

    Then no need to expose anything else, which as a bonus, gives you
    the complete freedom of reimplementing this :class:`~.PersistDataManager`,
    if you ever need to, without people using your class noticing.

    If one is worried about the length of each module then, what you can do
    is move both module in their own package, like :pkg:`kp_scrapers.persist`
    and add at the package::

       # kp_scrapers/persist/__init__.py
       from .spider import PersistSpider

    It is then clear what the package API is. And then all you need is
    to move both persist_spider.py and persist_data_manager.py to
    ``persist/spider.py`` and ``persist/manager.py`` for example (with
    shorter, `_` less, names). But remember the
    `Zen of Python <https://www.python.org/dev/peps/pep-0020/#the-zen-of-python>`_
    and its golden rule: *Flat is better than nested*.

    Oh and by the way for shortness and clarity, I would rename this
    ``kp_scrapers.spiders.bases.stateful.StatefulSpider`` and
    ``kp_scrapers.spiders.bases.stateful.manager._StateManager``.
"""

from __future__ import absolute_import, unicode_literals
from datetime import datetime, timedelta
import json
import logging
import os

from scrapy.utils.project import data_path

# TODO don't use start import
from kp_scrapers.lib.date import create_str_from_time, may_parse_date_str


logger = logging.getLogger(__name__)


class PersistDataException(Exception):
    pass


class PersistDataManager(dict):
    """
        Dict like object that persist data as json in file on SH

        Usage example is to persist the last execution of a spider
    """

    def __init__(self, filename, save_exec_time=False, *args, **kwargs):
        super(PersistDataManager, self).__init__(*args, **kwargs)
        if filename == '':
            raise PersistDataException('Filename required to persist data on SH')
        self.file_path = data_path(filename + '.json')
        logger.info('Using persistent file : ' + self.file_path)
        self._load()
        if save_exec_time:
            self['spider_exec'] = str(datetime.today())

    def save(self):
        with open(self.file_path, 'w') as f:
            try:
                json.dump(self, f)
            except ValueError:
                logger.error('Cannot serialize json file {}'.format(self.file_path))

    def clean_file(self):
        self.clear()
        try:
            os.remove(self.file_path)
        except IOError:
            logger.error('{} does not exist'.format(self.file_path))

    def get_last_spider_exec(self, day_diff=2):
        try:
            # Not sure if spider_exec hold a date
            d = may_parse_date_str(self.get('spider_exec'))

        except (TypeError, NameError, AttributeError):
            logger.warning('No spider_exec')
            return None
        else:
            # We remove n day just to be sure we have safe infos
            return d - timedelta(days=day_diff)

    def get_last_spider_exec_strfmt(self, frmt, day_diff=2):
        time = self.get_last_spider_exec(day_diff=day_diff)
        if time is None:
            return None
        return create_str_from_time(time, format=frmt)

    def _load(self):
        try:
            logger.debug('Deserializing file {}'.format(self.file_path))
            with open(self.file_path, 'rb') as f:
                self.update(json.load(f))
        except IOError:
            logger.error('{} does not exist'.format(self.file_path))
        except ValueError:
            logger.error('Could not deserialize to json file {}'.format(self.file_path))

    @staticmethod
    def delete_spidersfiles():
        dir_path = data_path('')
        logger.debug('Path of .scrapy dir == [%s]' % dir_path)
        display_list = os.listdir(dir_path)
        logger.debug('{%s}' % str(display_list))
        file_list = [f for f in os.listdir(dir_path) if f.endswith('Operator.json')]
        for f in file_list:
            logger.info('We delete file [{}]'.format(f))
            try:
                os.remove(f)
            except OSError as e:
                logger.error('Error: %s - %s.' % (e.filename, e.strerror))
