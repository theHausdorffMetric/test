===============
Getting started
===============

In this section we will configure, install and tweak everything you need to be up and
running for using or building your own scrapers. Many steps are hidden behind
scripts so one can quickly be efficient within the repository and focus on the data,
but feel free to go a little deeper and dive into the automation.


Prerequisites
=============

Before we begin, make sure you have the following tools installed (should already be the case
by default). We also recommend using SSH_ to simplify interaction with Github.

* Python 3.6 or newer
* pip 10.0 or newer
* Docker 19.03 or newer

Several Python development libraries are also required:

* Debian-based

  .. code-block:: bash

     $ sudo apt-get install build-essential libssl-dev libffi-dev libxml2-dev libxslt1-dev zlib1g-dev

* OS X

  .. code-block:: bash

     $ brew install libxml2 libxslt libffi


Installation
============

First, clone the repository and change your working directory to the repository root. The following
steps assume the usage of virtualenvwrapper_ for creating an isolated project environment.
Feel free to use your own tooling if convenient.

To create a virtualenv and install all dependencies:

.. code-block:: bash

   $ mkvirtualenv -a . -r dev-requirements.txt --python=`which python3.6` kp-scrapers

Once the environment has been successfully setup, confirm it is properly configured and installed:

.. code-block:: bash

   $ scrapy list

If properly installed, the names of all spiders in the project will be printed, each on a separate
line. If this is not what you see, ask a developer for assistance.


Post-installation
=================

We use `git-hooks`_ and `pre-commit`_ for automating our linting process. `git-hooks` executes our
linting scripts (hooks) when checking-out, committing and pushing. These linting scripts are in turn
defined by configuration files with `pre-commit`. Feel free to dive into the files if interested.

To install `git-hooks`_, we recommend downloading the
`latest binaries <https://github.com/git-hooks/git-hooks/releases/latest>`_ and moving the
uncompressed file to a directory specified in your ``$PATH`` variable.

`pre-commit`_ should already be installed as specified in `dev-requirements.txt`.


.. _git-hooks: https://github.com/git-hooks/git-hooks/
.. _pre-commit: https://pre-commit.com/
.. _kp-scrapers: https://github.com/Kpler/kp-scrapers
.. _SSH: https://help.github.com/articles/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent
.. _virtualenvwrapper: https://virtualenvwrapper.readthedocs.io/en/latest/
