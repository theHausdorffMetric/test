=========================
Advanced scraping options
=========================

Some websites don't like to be scrapped and have implemented counter-measures
to harden the scraping process. Here are a few tips and tricks to cope with
those 10% special cases.

Hiding behind a proxy
=====================

Not all sources will happily let a bot consume their website as you develop.
One workaround is to use a proxy and hide your identity. it's less effective
than working on offline copy but simpler and actually more realistic.

`Original idea and Credit <http://pkmishra.github.io/blog/2013/03/18/how-to-run-scrapy-with-TOR-and-multiple-browser-agents-part-1-mac/>`_

```Bash
brew install tor polipo
# start tor
tor
polipo -c tools/polipo.config
```

Sorry linux users, you are very welcome to contribute your installation steps.


Rotating your user agent
========================

A user agent is a client application used by an end user, typically for a
network protocol such as HTTP or FTP. For example :

```
Mozilla/5.0 (X11; U; Linux i686; fr; rv:1.8.1.1) Gecko/20060601 Firefox/2.0.0.1 (Ubuntu-edgy)
```

Changing the user agent your spider uses between each scraping makes it
harder for the site you scrap to spot a bot behavior. Add this line in
your spider class to enable the rotation of user agent::

    custom_settings = {
            'USER_AGENT_ROTATION_ENABLED': 'True'
        }


.. warning::
    Add examples for rotating agent use cases

**Work in progress**

Please contribute to this section if you encountered security measures
and a way to bypass them on a website you scrapped.
