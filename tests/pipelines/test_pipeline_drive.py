# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from unittest import TestCase

from nose.tools import raises
from scrapy.exceptions import NotConfigured

from kp_scrapers.pipelines.drive import DriveStorage


def _drive_settings(status):
    return {'KP_DRIVE_FEED_URI': 'Zee_Folder_ID', 'KP_DRIVE_ENABLED': status}


# NOTE could be more generic and moved to `mocks` module
class SpiderFactory(object):
    name = 'FakeSpider'
    project_id = 434
    job_id = '3939846'


class _FakeSpider(SpiderFactory):
    settings = _drive_settings(True)


class _FakeDisabledSpider(SpiderFactory):
    settings = _drive_settings(False)


class _FakeInvalidSpider(SpiderFactory):
    settings = _drive_settings('foobar')


class DriveStorageTestCase(TestCase):
    def test_enabled_settings_are_valid(self):
        self.assertIsNone(DriveStorage(None)._validate_settings(_FakeSpider().settings))

    @raises(NotConfigured)
    def test_disabled_settings_are_invalid(self):
        DriveStorage(None)._validate_settings(_FakeDisabledSpider().settings)

    @raises(NotConfigured)
    def test_wrong_settings_are_invalid(self):
        DriveStorage(None)._validate_settings({})

    @raises(NotConfigured)
    def test_empty_settings_are_invalid(self):
        DriveStorage(None)._validate_settings({})
