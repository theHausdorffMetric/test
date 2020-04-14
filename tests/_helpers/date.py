# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import datetime as dt


class DateTimeWithChosenNow(dt.datetime):

    chosen_now = None

    @classmethod
    def now(cls):
        return cls.chosen_now

    @classmethod
    def utcnow(cls):
        return cls.chosen_now
