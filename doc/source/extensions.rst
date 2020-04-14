=================
Extensions
=================


Going through the code, you will find several extensions we added
alongside scrapy's default features.


Spiders introspection
=====================

One thing we added is spider introspection. Our spiders are generally
fairly simple, but we have quite a lot of them. It is not necessarily
easy to find out what does what:

- What data is being retrieved ?
- What commodities the data retrieved relates to ?
- Which platform is concerned with the data retrieved ?
- *etc.*

So we added an additionnal command to the :command:`scrapy` tool:
:command:`describe`::

    $ scrapy describe

When invoked, it will write down a machine readable description of all
the spiders in the project. The description contains the spider name
alonside with some metadata. The available metadata so far are:

:commodities:
   A list of string tell for which commodities the spider may retrieve
   data for.
:category:
   The category of data source the spider scrape. So far, admissible values
   are:

   - agents: reports received from agents and usually stoerd on the drive
   - ais: for AIS / ETA providers;
   - slot: for installations delivery slots;
   - operator: for cargo delivery at specific installations, and more
     generally their inbound and outbound flows, whether it is through
     pipelines, vessels, *etc.*
   - port-authority: for port schedules and port movements, sometimes
     for cargos also.
   - registry: for players and vessel dynamic informations, like players
     who owns them, name, MMSI, flag changes, *etc.*
   - charter: for sources that deal with charterer reports
   - contract: groups spiders that perse contracts between two thrid parties,
     like Bill of Ladings
   - price: financial sources
   - weather: every weather source that could impact imports/exports

   The list of categories is available as the
   :class:`~lngspider.markers.SpiderCategory` enumeration


Stateful Spiders
================

Sometimes a website or an API requires our spider to be stateful, which
means we need to keep some informations from one spider run to the other.

Scrapy provides an extension to cope with this kind of situations called
DotScrapy, we further wrapped it to ease its use. One only needs to
inherit from the :class:`~kp_scrapers.spiders.bases.persist_spider.PersistSpider`.


Middlewares
===========

We developped or contributed to several scrapy
:ref:`spider middlewares <topics-spider-middleware>`.

We built or adapted several middleware to fulfill some of our needs, like:

* runtime job metadatas
* statistics gathering
* cache requests and store responses to AWS S3 buckets


Comprehensive extensions API
============================


.. toctree::
   :maxdepth: 1

   extensions/metadata.rst
   extensions/stateful.rst
   extensions/statistics.rst
   extensions/history.rst
