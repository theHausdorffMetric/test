import json
import os


__package__ = 'kp_scrapers'
__version__ = '66.29.0'


def _cache():
    """Stand out the cache filename."""
    return os.path.join(os.path.dirname(__file__), 'meta.json')


def memoize(func):
    def _inner(use_cache=False):
        """Dump/load static data to disk.

        Args:
            use_cache(bool): false by default because this is most of the time
                             a useless optimisation

        """
        cache_path = _cache()
        if use_cache and os.path.isfile(cache_path):
            with open(cache_path, 'r') as fd:
                return json.load(fd)

        data = func()

        with open(cache_path, 'w') as fd:
            json.dump(data, fd)

        return data

    return _inner


@memoize
def spiders(use_cache=False):
    """Scan the project to yield spider properties.

    Args:
        use_cache(bool): false by default because this is most of the time a useless optimisation

            {
                'category': ['registry'],
                'commodities': ['cpp', 'lng', 'lpg', 'oil'],
                '_type': 'spider',
                'enabled': True,
                'name': 'Equasis'
            }

    """
    # we don't want the project and its dependencies to be imported when we
    # only want to read the version (like in `setup.py`)
    from kp_scrapers.commands import run_scrapy_command  # noqa

    return run_scrapy_command('describe', '--filter enabled:true --silent')
