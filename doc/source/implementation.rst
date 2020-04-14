==============
Implementation
==============


**WORK IN PROGRESS**


Repository layout
=================


.. code-block:: Bash

    - doc/                          # Sphinx based documentation
    - githooks/                     # Git hooks scripts
    - circle.yml                    # Continuous integration definition
    - tox.ini                       # multi-env testing - unused but will help for Py3 transition
    - tests/
        |_ _fixtures/
        |_ _helpers/
        |_ lib/
        |_ models/
        |_ spiders/

    - LICENCE
    - setup.py
    - setup.cfg                     # Python tooling standard configuration
    - dev-requirements.txt
    - kp-requirements.txt.crypt     # private deps encrypted
    - Dockerfile                    # base Dockerfile to run spiders
    - scheduling/*.yml              # periodic jobs declaration
    - scrapinghub.yml
    - Makefile
    - tools/                        # useful scripts
        |_ cli/                     # cli helpers to work on kp-scrapers
        |_ devops/                  # build-related scripts
        |_ watch-test.py/           # run tests on change
        |_ sling/gmail2drive.gs     # Google script to copy mails to drive

    - scrapy.cfg
    - local_settings.tpl            # private settings template you can use
    - kp_scrapers/
        |_ business.py              # Kpler or maritime related and specific knowledge
        |_ constants.py
        |_ settings/                # Scrapy settings
        |_ commands/                # Scrapy custom commands - refer to the documentation for more
              |_ describe.py        # list all the spiders and their metadatas
        |_ lib/
        |_ models/
              |_ ...
        |_ spiders/
              |_ ...


Initialize a new spider
=======================

To create a new spider for a data source, create a new module in the appropriate category under
`kp_scrapers.spiders`, for example:

.. code-block:: Python

    # -*- coding: utf8 -*-

    """Some module-level explanations."""

    from scrapy.spiders import Spider

    from kp_scrapers.models.items import SomeItem
    from kp_scrapers.spiders.<category> import CategoryMixin

    class MySpider(CategoryMixin, Spider):
        name = 'SpiderName'
        start_urls = [ '...' ]

        def parse(self, response):
            yield SomeItem(key=value, ...)

.. warning::

  When defining your spider class, inheritance order matters. We've modified how Scrapy uses the
  `custom_settings` class attribute, which was originally used to override project settings just
  for that spider. The base `scrapy.Spider` class will reset your spider's custom settings if the
  inheritance order is swapped.

  For more details on changes to the `custom_settings` class attribute, see `Customise a spider`_

Customise a spider
==================

Sometimes, there may be a need to change settings just for that spider. Scrapy's documentation
specifies to assign 'custom_settings' spider class attribute with a dictionary of key-value pairs
for the settings. However, we've overhauled and extended much of scrapy's framework to have
extended settings categories, ordered by precedence from least to most:

* `custom_settings`: now used as a catch-all if a setting does not belong in the categories below
* `category_settings`: tag spiders appropriately for Datadog metrics
* `spider_settings`: override every other settings with these

Reference: `spiders.bases.markers.KplerMixin.update_settings`

Deprecate a spider
==================

Scrapers and sources sometimes go deprecated (website unavailable, spider no
longer suitable, lack of information, ...). We want to avoid spending more time
on them, the uncertainty of spiders state and usage and to pollute the
repository with un-maintained code.

We introduce a simple way to mark a spider as deprecated, preventing it from
running, and tagging it as such for tools like our `scrapy describe`.

Note that we tried to stick to the `marker` framework as we believe it holds
the same _meta-reasonning_.


.. code-block:: Python

    from scrapy import Spider

    from kp_scrapers.spiders.bases.markers import DeprecatedMixin

    class MySpider(DeprecatedMixin, Spider):
      # done. it can no longer be instanciated and has now the attribute `deprecated`
      pass


Note that if tests are associated with this spider they will break the build
since the spider is now raising an exception. The whole point of this framework
is to disable the spider without deleting the code, so removing tests doesn't
really make sense. However `nose` gives us the `skip` helper to sort the
situation out:

.. code-block:: Python

    import unittest

    @unittest.skip('helpful message to understand why it was deprecated')
    class MyDeprecatedTestCase(unittest.TestCase):
      # [ ... ]


TODO: Talk about

- Links to Scrapy ressources
- Categories
- Base spiders
- Lib
- Models
