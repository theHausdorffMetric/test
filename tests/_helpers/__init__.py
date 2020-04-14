# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
from collections import namedtuple
import functools

from nose.tools import eq_


__test__ = False


TestIO = namedtuple('TestIO', ['msg', 'given', 'then'])


def with_test_cases(*cases):
    """Readable shorthand to apply use cases table to a unit function.

    Just decorate with a set of `TestIO` cases and they will be all evaluated.

            @with_test_cases(TestIO('something should happen', given='this', then='that'))
            def test_something(self, given):
                # given is `'this'`
                # the returned value of `process` will be compared to `'that'`
                return process(given)

    """

    def _decorator(func):
        @functools.wraps(func)
        def _inner(klass):
            for usecase in cases:
                result = func(klass, usecase.given)
                # let tester provide contextual information in error message
                err = 'given {given} -> {result} != {then}'.format(
                    given=usecase.given, result=result, then=usecase.then
                )
                eq_(result, usecase.then, usecase.msg + ': ' + err)

        return _inner

    return _decorator
