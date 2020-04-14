============
Mail spiders
============

With new partnerships and existing mailing lists we receive many email reports that need to be
processed after being forwarded to them. These reports can contain, not exhaustively, the following:

* Spot charters
* Vessel lineups
* Fixtures
* Bills of lading


Abstract
========

Previously, such email reports were manually processed by analysts, sometimes by hand, sometimes
with a script. This was not efficient for a number of reasons:

* no centralisation/sharing of scraping, market knowledge
* little to no monitoring/scheduling exposed for both devs and analysts
* scripting/debugging by analysts without visibility from developers

There are also a number of requirements to address:

* analysts need to be able to write/share scripts easily
* analysts want visibility over how/when data is extracted from email reports

To address this, the following solution/workflow was adopted:

* Provide the necessary framework to interface with a mail inbox with `kp-scrapers` package
* Provide a "staging" Jupyter server for implementing/sharing scripts written with handy utility
  functions from `kp-scrapers` so that analysts only need to be concerned purely with parsing
  and not the framework
* Once an analyst is satisifed with the results, hands it over to a dev to formalise as a spider
* Schedule the spider on SHUB, notify via Slack when reports are successfully parsed and
  exported into a Google Sheet (for analyst vetting/visibility)
* Once satisified, analysts will shift the data to the respective folders for the relevant
  ShipAgentXls/SpotCharter/etc. spider to pick up

Thus, `MailSpider` was designed to abstract the process of obtaining emails, into a convenient
spider class inherting from the base `Spider`, yielding items in exactly the same fashion as that
of an ordinary base spider.

To enable export of items to Google Drive, simply specify `KP_DRIVE_ENABLED = True` in
`spider_settings`. By default, exported items are stored in the `kp-datalake` folder here_
(access restricted). For more details on how `spider_settings` are used, see
`Implementing a spider`_.


Requirements
============

* A Google account configured in your settings. Until we get a bot account, we're using
  `song@kpler.com` by default. Configure a custom account in `settings.py`: ::

    GMAIL_USER = 'someone@kpler.com'
    GMAIL_PASS = 'hunter2'

* Admin Google service account credentials for Drive export. This is required for accessing
  Google's API, ask a dev for access: ::

    GOOGLE_DRIVE_DEFAULT_USER = 'someone_else@kpler.com'
    GOOGLE_DRIVE_PRIVATE_KEY = 'pkey'
    GOOGLE_DRIVE_PRIVATE_KEY_ID = 'pkey id'


How to implement
================

Initialise a `MailSpider` as follows. The only requirement is it must `yield`, not `return`:

.. code-block:: Python

    # -*- coding: utf8 -*-

    """Zee mail reports spider."""

    from __future__ import unicode_literals

    from kp_scrapers.models.items import EtaEvent
    from kp_scrapers.spiders.agents import ShipAgentMixin
    from kp_scrapers.spiders.bases.mail import MailSpider

    class ZeeSpider(ShipAgentMixin, MailSpider):
        name = 'Zee'
        provider = 'Zee Market Research'
        version = '1.0.0'

        # see `Implementing a spider` for more details
        spider_settings = {
          'KP_DRIVE_ENABLED: True,
        }

        def parse_mail(self, mail):
            """Parse email report.

            Args:
              mail (Mail): Mail object, see `lib.services.mail.Mail` for details

            Yields:
              EtaEvent:

            # do something on `mail`

            yield EtaEvent(key=value, ...)


By default, each Scrapy job will be exported as a single Google Sheet in the `kp-datalake` folder.
Each Scrapy item yielded will constitute one row in the spreadsheet.
