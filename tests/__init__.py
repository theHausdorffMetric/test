# -*- coding: utf-8 -*-

# garantee `import unittest.mock` will work
from __future__ import absolute_import, unicode_literals

import backports.unittest_mock


backports.unittest_mock.install()
