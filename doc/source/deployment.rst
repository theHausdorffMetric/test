==========
Deployment
==========

This walktrough will take your spider in production and make its data available on the Kpler
platform.

Register new data providers
===========================

TODO clarify motivations

Many scripts will try to lookup data provider description when processing items
from Scrapers. It mainly acknowledge the source is known and active.

If such data provider doesn't already exist, just insert a new row in the
relevant platform databases, like the one below (illustrated from a port
authority data source).

.. code-block:: SQL

      INSERT INTO provider
        (name,          -- human readable explicit name
         shortname,     -- program usuable name
         active,
         positions,     -- is data related to positions ?
         port_calls,    -- is data related to port calls ?
         type,          -- Port Authority, AIS, ...
         lng_only,
         added_by)
        VALUES ('Bejaia Port Authority', 'Bejaia', TRUE, FALSE, TRUE, 'Port Authority', FALSE, 'Xavier Bruhiere')


Note that most of the loaders on the ETL will set by themselves the data provider given the item they process.


Deploying on Scrapinghub
========================

First get your versioning straight: pull upstream, rebase properly from
`master` and merge. Also, clean after yourself Github PR and Jira Ticket.

To release your spider, all you need is to deploy it on the production
project::

    $ # deployment version is automatically pulled from your git branch
    $ ./tools/cli/deploy.sh production

Then you need to schedule it. Each time your spider runs we give away a
few tenth of cents to ScrapingHub. It is not much, but considering the
number of request we send out to the cloud every day, it adds up to
quite a lot in the end. So be responsible when you schedule your
spider: if the website is only refreshed twice a day, no need to schedule
it every 10 or 15 minutes, it's just a waste of resources.

You can either use Scrapinghub interface or do it from your terminal. Just edit
the specification of your spider in `scheduling/<category>.yml` like so :

.. code-block:: yaml

        jobs:
          - spider: MySpider
            description: 'Iscrape website'
            crons:
              - '*, *, *, *, */30'
            args:
              foo: bar
            tags:
              - 'category:ais'
              - 'commodity:lng'

Then you can synchronise it with Scrapinghub periodic jobs. First export
your Scrapinghub API key SH_API_KEY_ in your env::

    $ export SH_API_KEY='<MY_SH_API_KEY>'

Note that if such spider is already scheduled, it will be
overwritten by the new configuration::

    $ ./tools/cli/kp-shub batch-schedule --config ./scheduling/ais.yml -p production -s 'MySpider'

Next step is to tell an ETL how to fetch the data stored on Scrapinghub.

TODO: document dynamic parameters and monitoring


Deploying on the ETL
====================

TODO: DEPRECATED - update it

Head to `kpler-deployment` repository and make sure you are up-to-date.
The goal is to update every relevant crontab of the **ETL** server so that we
spawn some jobs to load in database `scrapy` results.
Start by testing the deployment on staging : `$EDITOR ./crontab/crontab_*_staging`.::

    # scrapy job pooling
    {{ cron spec }} { export SH_PROJECT_ID={{ scrapy env id }} ; kpler_extract "ais_loader -s --spider {{ spider name }}" --no-lock ; }

    # dump loading
    {{ cron spec }} DB_STATEMENT_TIMEOUT=30000 kpler_extract \
    "ais_loader -l -m 18 -t 110 -x 5 --spider {{ spider name }}" \
    --lock-timeout 58  # server job lock

Finally deploy your changes::

    $ ansible-playbook etl-deploy.yml --limit "staging" --tags crontab

At this point, `ais_loader` will complain it doesn't know your spider. To fix
this, switch to `lng-data` repository and edit
`etl/extraction/ais/ais_loader.py`::

    SPIDER_TO_PROVIDER_SHORTNAME_MAP = {
      'MarineTrafficAIS2': 'MT_API',
      # existing spider map [ ... ]
      '{{ spider name as on SH }}': '{{ spider id as in vessel_list }}',
    }

Finally write an sql script to update each Postgres tables `provider` (use
`DESCRIBE provider` and ask if you are not sure about the fields to fill).

TODO: document this release

The script should now periodically fetch spider's output and load it in
database. One way to monitor the process is reaching
logmatic_, filter on `Hostname`, `Data Provider` and
control the logs.

.. _SH_API_KEY: https://app.scrapinghub.com/account/apikey
