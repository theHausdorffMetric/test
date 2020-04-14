========================
Monitor spider execution
========================

Running hundreds of spiders requires historical monitoring for discovering
insights and definining alerts. We do so through Datadog_, like for the rest of Kpler stack, and a custom
(open source) extension_.


Scrapy Extension
================

You can refer to the project's README_ to learn more about the extension, what
it does and how you can use it with the spiders.

Dashboards
==========

Although you are encouraged to create your own dashboards and explore metrics,
we have a few conventions and already in place useful ones.

When creating a long-lasting dashboard on Datadog, try to clearly identify it
by prefixing the title with *'[DC]'* (DC stading for Data Collection), so that we can order/filter them
efficiently.

* Global health overview_

* Port authorities behavior_

* AIS sources quality_

.. _README: https://github.com/Kpler/scrapy-datadog-extension/blob/master/README.md
.. _extension: https://github.com/Kpler/scrapy-datadog-extension
.. _Datadog: https://datadog.com
.. _overview: https://app.datadoghq.com/screen/221935/shwip-data-sourcing-screenboard
.. _behavior: https://app.datadoghq.com/dash/301699/etl-port-authorities-loading
.. _quality: https://app.datadoghq.com/dash/272224/etl-data-sources-quality-wip
