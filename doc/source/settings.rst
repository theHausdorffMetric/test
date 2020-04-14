=============
Configuration
=============


Project settings
----------------

Scrapers runtime can be configured through:

- `Environment variables <https://12factor.net/fr/>`_
- `Scrapy settings`_
- `Scrapinghub settings`_

The strategy is to store everything in :module:`kp_scrapers.settings`. Except:

- Extensions for which we decided otherwise for various reasons (third party,
  compatibility, ...). Refer to the appropriate section.
- Secret settings like credentials. Since we don't commit them, we need to keep
  them activated out of the repo. In production we use Sceapinghub settings and
  their Gui. Locally the code tries to import `local_settings` module.

Not that removing extensions mandatory settings deactivate them at
initialization time without stoping spiders. It is actually the way to if you
want to stop an extension without removing it from the lists defined in
`kp_scrapers.settings` (temprary fix for example, not something to be committed
in the code).

::

    $ cp local_settings.tpl local_settings.py  # it is ignored in `.gitignore`
    $ # done - edit it to your convenience


Specific utilities
------------------

Add the following settings in ScrapingHub interface or settings.py before deploying.

* Activate Crawlera

::

    CRAWLERA_ENABLED = True
    CRAWLERA_APIKEY = "XXXXXX"

* Request/response Cache and Raw Response Files

::

    HISTORY = True
    HISTORY_EPOCH = False
    HISTORY_STORE_IF = 'history.logic.StoreAlways'
    HISTORY_RETRIEVE_IF = 'history.logic.RetrieveNever'
    HISTORY_BACKEND = 'history.storage.S3CacheStorage'
    HISTORY_S3_BUCKET = 'scrapinghub-data-%(project_id)s'
    HISTORY_USE_PROXY = 0
    HISTORY_SAVE_SOURCE = '%(name)s/%(time)s--%(job_id)s'

* Save Items in S3 in JsonLine File

::

    AWS_ACCESS_KEY_ID = 'XXXXXX'
    AWS_SECRET_ACCESS_KEY = 'XXXXXX'
    FEED_URI = 's3://scrapinghub-data-%(project_id)s/%(name)s/%(time)s--%(job_id)s/%(time)s--%(project_id)s--%(name)s--%(job_id)s.jl'

* Send metrics to Datadog

::

    DATADOG_API_KEY = "XXXXXX"
    DATADOG_APP_KEY = "XXXXXX"



.. _`Scrapy settings`: <https://doc.scrapy.org/en/latest/topics/settings.html
.. _`Scrapinghub settings`: https://app.scrapinghub.com/p/434/job-settings/standard
