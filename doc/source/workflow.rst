========
Workflow
========

Product
=======

- Make sure Airtable_ is up-to-date (state, product owner, source
  information...). If not, gather and fill all that can be helpful.
- Then discuss with your manager or product owner to create a Jira card. Again,
  make sure the requirements are as clear as possible, especially if you are
  not alone to work on the task. Make sure you understand everything that is
  expected from a data and business point of view and ask questions to the
  analysts if it's not the case.
- Then head to `kp-scrapers` repository and *pull the last changes*.
  Development goes fast. Don't assume anything, just pull the changes first.
  Finally you can start a new branch to work on your feature following this
  pattern: `<type of jira card>/<jira ID>-<a-few-words>`. Branch names are
  mostly consumed by humans so make them helpful.

TODO: add steps for Gdrive PA guidelines

Observe
=======

Observe how the target website works. The questions you might want to answer
before even starting to code are:

- Does the website requires you to log in ?
- How is the login performed ?
- Does it uses forms ? Which are they ?
- Is the website stateful ? (storing lots of data in forms or cookies
  from one request to the other).
- Is some of the page content built by javascript code ? Does that code
  query some API ? Can you bypass the "human readable" website then ?

Once you have a pretty good idea about the anwser to these questions and
of the website's kinematics (which requests need to be performed in what
order to reach the target pages), then you can move on.


Try
===

Now that you have your target pretty much figured out, start writing your spider.
Those are grouped by categories closely related to the type of data they bring.
It helps us segment everything, from monitoring to understanding of the data
flow. You can check them and start coding in:

    kp_scrapers/spiders/<catagory>/<spiders>

Check data source of a few existing spider, check how the extraction
pipeline is built. Use git blame to make sure you take inspiration from
the most up to date code. Take example on existing spider, reach scrapy_
documentation and start coding. Once you have a version able to perform
the required queries to reach the target page(s), you can try to run
your spider, with:

    scrapy crawl SPIDER_NAME

Unless you have some crawling logic to implement, it may not be necessary
to deal with parsing the page content at first. Just make sure you have
the right page with the expected content: is it the same as when you browse the
website within your browser. Dumping the received responses to files might
help.


Extract
=======

To work on the webpage parsing side of your spider, you may want to
download at last one of the target pages to a file and use it as a
fixture. You can then easily build tests and work on the data
extraction without hitting the network all the time:

- This is particularly important when the website has request rate
  limiting policies enforced;
- It is always faster to read a local file than to connect to a remote
  server and download a webpage;
- Also the fixture(s) you downloaded might help you, or someone else,
  understand why a spider breaks at some point: it gives a reference to
  compare the currently scraped page with what the spider was built
  against; helps understand the current spider behaviour and how it
  should be modified.
- It will also help retain compatibility between versions a of webpage.
  Indeed you never know why from some time a webpage is not exactly how
  it used to be. Unless the website has obviously been completely
  overhauled, the change may simply be the consequence of a bug that
  will later be solved or *disappear as it appeared*. Thus *fixing*
  the bug without retaining backward compatibility is just introducing
  a bug in *our code* for the weeks come !


Stage
=====

Once you are done, it is necessary to deploy your spider to one of our test
projects on scrapinghub. The ScrapingHub is a complicated mecanic and
sometimes your spider, that works perfectly fine on you computer, will
not work there.

The differences generally revolves around some settings and middlewares
enabled there that you may not want to use on your development environment.
Refer to the settings section since our current approach tries to minimize this
risk.

First grab your scrapinghub API key_ from your profile page and export it as
`SH_API_KEY`. It will allow the different script to interact with the service.

To deploy your spider to ScrapingHub, you need to run the following
command::

    $ ./tools/cli/deploy.sh --help
    $ ./tools/cli/deploy.sh <PROJECT> [some-push-message]

Where ``<PROJECT>`` is either a ScrapingHub project ID or an alias as
defined in the `scrapinghub.yml` file you will find in this project's
root directory (see under the `projects` key).

It's important to note that you can leave it empty and automatically deploy to
the same environment you created with `./tools/cli/kp-shub create`, on Kpler
staging organization. It is meant to
make tools work the same way by default and save you some keystrokes and brain
cycles. It also helps using a free organization for test purposes, isolated
from production resources.

As of now we use 3 different environments on the main Kpler org:

* `production`: the main target where production scrapers run and extract data
  that ends up on clients' dashboards
* `production-pdf`: basically the same except it is tailored to run scrapers
  that parse PDF files. You can refer to the specific section for more details.
* `playground`: a remote environment where you can test spiders in a very close
  approximation of what is in production. This is also a place where you can
  schedule periodic execution if you need to evaluate your work on a longer
  term.

On Kpler staging org you are expected to use `kp-shub create` to create individual
projects along your feature-branches and delete them with `kp-shub delete` after
merging;

Note:
    There is not point in periodically scheduling your spiders on the `playground`
    projects just run them manually, as you did on you computer, and ensure
    the data retrieved is identical to what you expected.

Once you are convinced you spider is operational, you can deploy it.


Advanced scrapers
=================

The workflow described here should cover most of the encountered sources
to scrap. If you need some tips and advanced techniques take a look at

.. toctree::
   :maxdepth: 1

   advanced.rst

.. _scrapy: https://scrapy.org/
.. _key: https://app.scrapinghub.com/account/apikey
.. _logmatic: https://app.logmatic.io/kpler#home
