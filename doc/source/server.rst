======================
Running spiders on EC2
======================

To repeat the introduction in the readme:

> Scrapinghub is occasionally down and for that reason a backup EC2 machine has been
> provisioned to run AIS scrapers as crons. This is a hacky work in progress.


Context
=======

The initial EC2 server, named `production-scrapers` and located in Ireland like
most of our servers, was built after a long-lasting interruption of service at
Scrapinghub. It was provisioned like any ETL. It means a linux-based AMI where
you ssh keys have been uploaded. But there is no playbook to automatically
recreate its current state and that's why you will find it stopped in our
inventory. Don't delete it :)

Spiders have been installed on it and a crontab has been written to schedule
AIS spiders as soon as the server comes online. It has been also configured to
run as if they were running on scrapinghub, i.e. in production. But we don't
have scrapinghub storage so items are only uploaded on S3, waiting for Data
Dispatcher to pick them up.

Since your ssh key should have been uploaded, you can just reach the instance
using `kpler@34.254.180.35`. Yes there is no DNS and yes if you are new at
Kpler there is a good chance your key is not there yet. You can use the
`ssh-authorize.yml` playbook to fix that.


Environment
===========

On top of Debian there are 3 important changes.

* Python 3.6.4 has been compiled from source as it is not part yet of the
  stable repository. This is mandatory to support f-strings used in
  kp-scrapers.

* A master version of kp-scrapers has been cloned in `/home/kpler` and a
  `kp-scrapers` virtualenv has beed created using this Python version with all
  dependencies installed. Logging on the machine as `kpler` will automatically
  activate it.

* A crontab replacing scrapinghub scheduler has been written and periodically start AIS spiders.

* It does so using `/home/kpler/run.sh` where environments is customised to fit
  production settings and scrapinghub runtime. It also wraps `scrapy` command
  to redirect logs in `~/logs`. Both this script and the crontab have an up to
  date version in `kp-scrapers` repository.


Limitations
===========

Since all of this has been done manually, you are strongly advised to check the
version in `kp_scrapres/__init__.py` and pull master if possible. Master should
always be the latest stable version in production and running it should be
safe.

We are in the process of migrating sources to data dispatcher and as a
consequence the crontab on the server is probably already outdated. The team is
aware of it and try hard to keep everything in sync.

As mentioned there is no playbook at the moment to reproduce the current setup.

Finally breaking changes sometimes ship so once the crontab has started, head
to datadog and make sure items start to flow again. The monitring on the
instance works like on scrapinghub so things should be up again.

Finally in case scrapinghub comes back online and the server is still running,
you will have spiders running twice at 2 different place. Since Data Dispatcher
filters for duplicates the consequence should be minimal. But when possible,
try to STOP (and only stop) the instance.


Next
====

The crontab approach has an obvious limitation. With close to 300 spiders, it's
hard to imagine maintaining it manually. Instead, I think it should not be too
hard to writea script to translate our scheduling folder into a proper crontab.
That way we would be able to maintain a consistent sync between scrapinghub
scheduling and the EC2 server.


The other big improvement is of course properly deploying it.

#### Idea 1 - Docker

Spiders are deployed inside Docker containers on Scrapinghub. An interesting solution would be then to re-use those containers and pass settings like on scrapinghub but this time using env variables.

**Pros**

- Only need to provision a server with Docker
- Same runtime than local and scrapinghub - easier to test, to run, safer to dpeloy and all _the Docker blabla_
- Easier to maintain - just pull the latest stable image
- Don't have to install Python **3.6**, which is not part of stable depots (and needs manual compilation

**Cons**

- Since the instance is dedicated to spiders we don't need much this isolation


#### Idea 2

Provision an image with the full project and just run them properly.

**Pros**

- It's halfway what we are currently doing, and we already have an AMI with Python 3.6


**Cons**

- Harder to provision and manage the installation of kp-scrapers. Not terrible either though and after the initial version of the playbook, there shouldn't be so much changes
- Different runtime than scrapinghub
