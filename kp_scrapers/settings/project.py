"""Scrapy project mandatory settings."""

import os


def __spiders(module):
    """Syntax sugar to hide spiders path layout.

    Examples:
        >>> __spiders('foo')
        'kp_scrapers.spiders.foo'

    """
    return 'kp_scrapers.spiders.%s' % module


BOT_NAME = 'spiders'

# default, can be (is) customized per spider or from SHUB
LOG_LEVEL = 'WARNING'

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# DO **NOT** put kp_scrapers.spiders.bases in here
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
__SPIDER_TYPES = [
    '_internals',
    'agents',
    'ais',
    'bunker_fuel_cost',
    'fleet',
    'canals',
    'charters',
    'contracts',
    'customs',
    'exchange_rate',
    'market',
    'operators',
    'port_authorities',
    'prices',
    'registries',
    'slots',
    'tropical_storm',
]
SPIDER_MODULES = [__spiders(mod) for mod in __SPIDER_TYPES]

# where to create new spiders using the genspider command
NEWSPIDER_MODULE = __spiders('others')
COMMANDS_MODULE = 'kp_scrapers.commands'

SHUB_SPIDER_TYPE = os.environ.get('SHUB_SPIDER_TYPE')
