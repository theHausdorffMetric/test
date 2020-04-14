================
Analytics server
================

Kplerlab is a notebook server. It stands as a platform for analysts and data-sourcing developers to
collaborate on writing spiders, and also as a platform to run python scripts and periodic jobs.
We use this platform to update analyst dashboard and data quality.
Anything related to data sources status or quality update could be scheduled here.

There are two ways to access the server: browser and ssh.

On browser
==========

Login address: http://analytics-legacy.dev.galil.io:8080

Current password: `ask a dev for the password`

SSH to the server
=================

The first time ssh to the server, you'll need to set ssh key and configure it beforehand, you could
reach to other developers if there's an issue. We store the keys in default folder of kp-deployment_.


- Configure it

Config file lives in ~/.ssh/config, your config contents for kplerlab could look like:

.. code-block:: bash

     Host kplerlab
     HostName analytics-legacy.dev.galil.io
     Port 20
     IdentityFile [PRIVATE_KEY_FILE]
     User admin

- SSH to the server

.. code-block:: bash

     $ ssh kplerlab


- Upgrade kp-scrapers package

As kp-scrapers is packaged and uploaded on pypi server, to install or upgrade on kplerlab, run the
following command with pypi url specified.


.. code-block:: bash

     $ pip install --extra-index-url https://$PACKAGECLOUD_READ_TOKEN:@packagecloud.io/kpler/stable/pypi/simple --upgrade kp-scrapers

.. _kp-deployment: https://github.com/Kpler/kp-deployment/tree/master/files/public_keys/default
