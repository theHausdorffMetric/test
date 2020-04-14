***********
Data models
***********


To allow a fairly generic processing of items from the point of view of
the ETL project, we define here the different types of data items and
what they are exepect to contain.

Some are deprecated and, while still documented here. Such items should not be
produced by newly written spiders and any issue identified with a spider that
yields such item should be seen as an opportunity to update the spider to newer
item formats.

Other are not used on their own but are to be included into others *master*
item types.

Each individual item type is describe in its own page below:

.. toctree::
   :maxdepth: 1

   items/cargo
