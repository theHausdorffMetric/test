import os


def env_is_true(env_key):
    """Scrapinghub might only expose strings when we need boolean.

    Checking for anything else than 'true' is useless but so cheap.

    Examples:
        >>> env_is_true('FOO')
        False
        >>> os.environ['FOO'] = 'true'; env_is_true('FOO')
        True
        >>> os.environ['FOO'] = 'yes'; env_is_true('FOO')
        True
        >>> os.environ['FOO'] = 'no'; env_is_true('FOO')
        False

    """
    return os.getenv(env_key, '').lower() in ['true', 'yes', 'y']


def is_shub_env():
    """Check if defined, whatever the value.

    Can be forced for development convenience.

    """
    return os.getenv('SHUB_JOBKEY') is not None or env_is_true('FORCE_SHUB_ENV')


def determine_env():
    """Determine which environment script is running on
    """
    _env = os.getenv('SCRAPY_PROJECT_ID')

    if not _env:
        return 'local'

    if '321191' in str(_env):
        return 'production'

    if '6932' in str(_env):
        return 'staging'
