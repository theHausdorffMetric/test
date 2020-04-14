#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""Hot lazy test runner.

Will watch for every file events within under the given directory (or the whole
project by default) and run tests if something happens to Python file.

It then assumes the `tests` directory replicates `kp_scrapers` layout, meaning
modifying `kp_scrapers/lib/date.py` will trigger nose to run against
`tests/lib/date.py`. This is naive and only serves to gauge this script
benefits for development.

"""

from __future__ import absolute_import, print_function, unicode_literals
import fnmatch
import hashlib
import logging
import os
import sys
import time

import click
import nose
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer


click.disable_unicode_literals_warning = True


def md5(fname, blocksize=4096):
    hash_md5 = hashlib.md5()

    with open(fname, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_md5.update(chunk)

        return hash_md5.hexdigest()


def search_test(pattern, root_search='.'):
    print('searching for ', pattern, 'in', root_search)
    for root, dirnames, filenames in os.walk(root_search):
        for filename in fnmatch.filter(filenames, pattern):
            logging.info('matched file: %s', filename)
            return os.path.join(root, filename)

    return None


class SpiderCode(object):

    tests_root = 'tests'

    def __init__(self, src_path):
        base, self.name = os.path.split(src_path)
        pattern = self.name if 'test' in self.name else 'test_{}'.format(self.name)
        self.test_src = search_test(pattern, self.tests_root)

    def test(self):
        if self.test_src is None:
            return logging.warning('no test files found...')
        elif '.py' not in self.test_src:
            return logging.info('not a Python file, skipping.')

        return nose.run(argv=[sys.argv[0], self.test_src, '-v', '--with-timer', '--with-doctest'])


class LazyTestRunner(PatternMatchingEventHandler):
    def __init__(self, *args, **kwargs):
        super(LazyTestRunner, self).__init__(*args, **kwargs)
        self.historical = {}

    def on_any_event(self, event):
        what = 'directory' if event.is_directory else 'file'
        logging.info('caught event @ %s %s', what, event.src_path)

    def on_modified(self, event):
        # TODO detect no change
        logging.info('caught modification event @ %s', event.src_path)
        spider = SpiderCode(event.src_path)

        file_id = md5(event.src_path)
        logging.info('file id: %s', file_id)
        if self.historical.get(event.src_path) != file_id:
            # either we never saw it or it changed
            self.historical[event.src_path] = file_id
            logging.info('running test for {}: {}'.format(spider.name, spider.test_src))
            ok = spider.test()
            # NOTE handle `SKIPPED` case
            logging.info('tests results: %s', 'SUCCESS' if ok else 'FAILLURE')
        else:
            logging.info('nothing new under the sun, skipping...')
            return


@click.command()
@click.argument('watch_roots', nargs=-1)
@click.option('-p', '--pattern', default='*.py', help='files pattern to watch')
@click.option('-t', '--timeout', default=5, help='observer timeout')
def main(watch_roots, pattern, timeout):
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
    )

    observer = Observer(timeout)

    # TODO default to module and tests, with os.path.exists
    event_handler = LazyTestRunner(patterns=[pattern])
    for path in watch_roots or ['.']:
        logging.info('watching @ %s' % path)
        observer.schedule(event_handler, path, recursive=True)

    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    main()
