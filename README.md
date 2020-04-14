<img src="https://web-intelligence-group.bitbucket.io/img/graph.svg" width="260" align="right">

# kp-scrapers

[![python-shield]](https://docs.python.org/3/whatsnew/)
[![scrapy-shield]](https://docs.scrapy.org/en/latest/news.html#scrapy-1-8-0-2019-10-28)
[![code-style]](https://github.com/python/black)
[![circleci]](https://circleci.com/gh/Kpler/workflows/kp-scrapers/tree/master)

> Gather data from the entire world, structure and validate it, and make it available for downstream advanced usage.

## Introduction

This project is built around the [`scrapy`][scrapy] framework, which provides a collection of
features designed to simplify our work in extracting data from a multitude of different sources.
For more details on `scrapy`, check out the official [documentation][scrapy-docs].


## Getting started

First, clone the repository and install project requirements:

```sh
mkvirtualenv -a . -r dev-requirements.txt --python=python3.6 kp-scrapers
```

In the above example, [`virtualenvwrapper`][venv] is used to scope the project environment.
Feel free to use your own setup if convenient. You may also source [`.env`][kp-local-env] in the
project root for access to commonly used aliases and a local streaming output.

Once installed, duplicate and rename [`local_settings.tpl`][kp-local-settings] to
`local_settings.py` to bootstrap the project with core settings. Thereafter, to run a scraper:

```sh
# basic example
scrapy crawl Bejaia --loglevel DEBUG

# some spiders require additional args to function; use `-a` option (repeatable)
scrapy crawl CorpusChristi --loglevel DEBUG -a username=foo -a password=bar

# you may also check out other `scrapy` subcommands if interested
scrapy --help
```

Don't forget to install [`git-hooks`][githooks] and [`pre-commit`][pre-commit] to automate linting:

```sh
git hooks install
pre-commit install
```

## Deployment

Our scrapers are exclusively hosted and executed on [Scrapinghub][shub],
leaving the hassle of scheduling to them. Consequently, most of the tooling in this project
is built around enabling ease of access and greater development velocity with Scrapinghub.

We currently maintain two persistent environments on Scrapinghub:
- [`production`][shub-prod] - scrapers yielding live data for downstream consumption
- [`staging`][shub-staging] - experiment with scrapers/extensions/infra here

[Docker][docker] is required for deployment on Scrapinghub. To deploy the project on `production`:
```sh
./tools/cli/deploy.sh production
```

However, Scrapinghub is occasionally down and for that reason a backup EC2 machine has been
provisioned to run AIS scrapers as crons. This is a hacky work in progress; more information is
available in the [documentation](https://doc.kpler.com/kp-scrapers/server.html).


## Further reading

The full [documentation][kp-docs] contains the nitty-gritty and best practices on writing, testing,
and deploying scrapers.

## Contributing

Find an issue or see anything missing ? Feel free to open a pull request and follow the guidelines
specified in the template description.

For further assistance, contact [`#crew-ds`][slack-channel].


## Resources

- [Datadog][datadog] - Metering and statistics
- [Scrapinghub][shub-org] - Kpler organization on Scrapinghub
- [Sentry][sentry] - Exception tracking
- [`#crew-ds`][slack-channel] - Where we live


[circleci]: https://circleci.com/gh/Kpler/kp-scrapers/tree/master.svg?style=shield&circle-token=e7e8f70a3107db25466c075e8f1b8983c8ffaa6d
[code-style]: https://img.shields.io/badge/code%20style-black-black.svg
[datadog]: https://app.datadoghq.com/dashboard/x9d-zs3-r4y
[docker]: https://docs.docker.com/install
[githooks]: https://github.com/git-hooks/git-hooks
[kp-containers]: https://github.com/Kpler/kp-containers
[kp-docs]: https://doc.kpler.com/kp-scrapers
[kp-local-env]: https://github.com/Kpler/kp-scrapers/blob/master/.env
[kp-local-settings]: https://github.com/Kpler/kp-scrapers/blob/master/local_settings.tpl
[pre-commit]: https://pre-commit.com
[python-shield]: https://img.shields.io/badge/python-3.6+-blue.svg
[scrapy]: https://scrapy.org
[scrapy-docs]: https://docs.scrapy.org/en/1.8/intro/overview.html#what-else
[scrapy-shield]: https://img.shields.io/badge/scrapy-1.8-blue.svg
[sentry]: https://sentry.io/kpler/scrapers/
[shub]: https://scrapinghub.com
[shub-prod]: https://app.scrapinghub.com/p/321191/spiders
[shub-org]: https://app.scrapinghub.com/o/288
[shub-staging]: https://app.scrapinghub.com/p/6932/spiders
[slack-channel]: https://kpler.slack.com/messages/crew-ds
[venv]: https://virtualenvwrapper.readthedocs.io/en/latest
