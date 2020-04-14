#! /usr/bin/env python
# -*- coding: utf-8; -*-

"""
Bump package version number.

Pattern: `x.y.z`

We increment `x` (major) by 1 each time data models evolve (and may break
assumptions downstream).
Otherwise spider changes bump `y` (minor) as wit may or may  not change the
_API_ of the repository.
Everything else is considered a patch and bumps `z.`

The bot needs proper Git configuration so run it in the following env:

```
export GIT_AUTHOR_NAME=XXX
export GIT_AUTHOR_EMAIL=YYY
```

where GIT_AUTHOR_NAME is the human-readable name in the 'author' field and
GIT_AUTHOR_EMAIL is the email for the 'author' field.

Note that the path of the script is as expected by the git webhooks that make
use of it. DONT MOVE IT if you don't know what you're doing.

"""

from __future__ import absolute_import, unicode_literals
import os
import subprocess
import sys

from six.moves import range


path = os.path.abspath(__file__)
PACKAGE_PATH = os.path.dirname(os.path.dirname(path))
PACKAGE_NAME = 'kp_scrapers'
VERSION_FILE = os.path.join(PACKAGE_PATH, PACKAGE_NAME, '__init__.py')
sys.path.append(PACKAGE_PATH)

# semantic versioning indexes in the split array of 'x.y.z'
MAJOR, MINOR, PATCH = range(3)


def _path_to_module(filepath):
    return os.path.relpath(filepath).replace('.py', '').replace('/', '.')


def get_version(pkg):
    pkg_version = __import__(_path_to_module(VERSION_FILE)).__version__

    return [cast_version_partial(i) for i in pkg_version.split('.')]


def cast_version_partial(partial):
    # not elegant but works and handles 'x.y.c-rc
    return int(partial.split('-')[0])


def inc_version(to_inc):
    version = get_version(PACKAGE_PATH)
    version[to_inc] += 1
    # reset to 0 lower-level version
    for i in range(to_inc + 1, len(version)):
        version[i] = 0

    new_version = '.'.join([str(v) for v in version])

    # overwrite version file
    lines = []
    with open(VERSION_FILE) as f_handler:
        for l in f_handler:
            if l.startswith('__version__'):
                lines.append("__version__ = '{}'\n".format(new_version))
            else:
                lines.append(l)

    with open(VERSION_FILE, 'w') as f_handler:
        f_handler.writelines(lines)

    return new_version


def run_command(command):
    """Run a shell command, log its output, and return the output """
    sp = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = sp.communicate()
    if err:
        raise RuntimeError("Subprocess {} returned an error: {}".format(command, err))

    return out


def module_changed(mod_name):
    return run_command('git diff HEAD~1 --name-only -- *{}/*'.format(mod_name))


def main():
    """Automatically bump project `__version__`.

    It tries to follow semantic format but the logic is slightly adapted :
    - Bump patch version when rest of the repo or lib code change
    - Bump minor version when spiders change
    - Bump major version when models change

    """
    # be careful of the checks flow below. We want `major` bump to take
    # precedence over `minor` and of course `path`.
    # default bump, anything but models or spiders
    version_level = PATCH
    if module_changed(PACKAGE_NAME + '/models'):
        version_level = MAJOR
    elif module_changed(PACKAGE_NAME + '/spiders'):
        version_level = MINOR

    new_version = inc_version(version_level)

    msg = (
        'Bump version number to {}\n\nAutomatically bumped by '
        '"tools/bump_version_number.py" [ci skip]'.format(new_version, __file__)
    )

    run_command('git add {}'.format(VERSION_FILE))
    run_command('git commit -m "{}"'.format(msg))
    run_command('git tag "{}"'.format(new_version))


if __name__ == '__main__':
    main()
