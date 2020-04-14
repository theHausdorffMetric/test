=================================
Associating Meta-datas to spiders
=================================

.. automodule:: kp_scrapers.spiders.bases.markers


Introspection Contract
======================

The following class defines the method a Spider class should implement
to be introspectable as expected by the `describe` command of the
`scrapy` tool.

.. autoclass:: kp_scrapers.spiders.bases.markers.KplerMixin
   :members:


.. warning::

   No spider should use any of this class method names for any other
   purpose than introspection.

   Make sure none of your spider method names clash with those.


Spider Categories
=================

Each spider may belong to exactly one category. All possible categories
are listed by the :class:`~kp_scrapers.spiders.bases.markers.SpiderCategory` class:

.. autoclass:: kp_scrapers.spiders.bases.markers.SpiderCategory
   :members:


Commodity Marker Classes
========================

The following class are marker classes for commodities. Simply inherit
from the one you need to ensure the corresponding commodity is returned
in the list of commodities your spider is concerned with.

.. autoclass:: kp_scrapers.spiders.bases.markers.CoalMarker
   :members:

.. autoclass:: kp_scrapers.spiders.bases.markers.LngMarker
   :members:

.. autoclass:: kp_scrapers.spiders.bases.markers.LpgMarker
   :members:

.. autoclass:: kp_scrapers.spiders.bases.markers.OilMarker
   :members:


Runtime metadatas extension
===========================

TODO
