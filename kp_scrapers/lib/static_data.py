# -*- coding: utf-8 -*-

"""Wrapping Remote storage for abstraction (future proofing, backend agnostic),
convenient local dev, fun and profit.


Context
~~~~~~~

Spiders sometime need to access Kpler-related logic, but we don't want them to
have access to our databases (no business logic, separation of concern,
security). Yet AIS clients for exemple sometimes need to know our fleet.

Solution
~~~~~~~~

That's why we expose a static dataset in spiders' environment and in local.
This dataset is remotely stored on S3 for reliability reasons, and use a cache
(without automatic invalidation though, except on reboot if you use the default
location) locally to avoid rate limit or even network access.

Legacy approach used to work with Scrapinghub collections. Although it's a
working alternative, we basically prefer to rely on AWS instead of a less
reliable, more specific, third party like SHUB.

"""

from __future__ import absolute_import
import csv
import json
import logging

from kp_scrapers.lib import utils
from kp_scrapers.lib.services import s3
import kp_scrapers.settings as settings


logger = logging.getLogger(__name__)

CACHE_PATH_TPL = '{base}/sh-cache.{coll}.json'
FLEET_S3_ARCHIVE = 'vessels-fleet.{version}.jl.gz'
STATIC_DATA_BUCKET = 'kpler-sh-data'
_BASE_LOCAL_CACHE = '/tmp'


class Collection(list):
    """Static data interface made available to spiders.

    Args:
        collection_name (str): will be used to find remote data and in the
                               cache filename
        index (str): field to use when querying by key. It is mostly legacy
                     inheritage from scrapinghub collection but it turned out to be quite
                     useful.
                     possible better interfaces: vessels.get(mmsi='1234566')

    """

    def __init__(self, collection_name, index):
        # setup internal `List` magic
        super(Collection, self).__init__()

        self.index = index
        self.name = collection_name
        self.cache_path = CACHE_PATH_TPL.format(base=_BASE_LOCAL_CACHE, coll=collection_name)
        # we only want to support S3 today, but if you need dynamic backend,
        # pass it at initialization and set it there.
        self._fetch = s3.fetch_file

    def load_and_cache(self, disable_cache=False):
        """Decide from where to populate internal list."""
        # try to load json (empty result if file doesn't exist) if caching is allowed
        cache = [] if disable_cache else utils.may_load_json(self.cache_path)

        if len(cache):
            # we got it, we're developing locally => use it
            logger.info('using local cache `{}`'.format(self.cache_path))
            self.extend(cache)
        else:
            logger.info('fetching remote datastore: {}'.format(self.name))
            # NOTE to keep the callback generic, there could be only one
            # argument (the full resource path), parsed by the callback
            data = list(self._fetch(STATIC_DATA_BUCKET, self.name))
            self.extend(data)

            if not disable_cache:
                logger.info('init cache with remote data')
                utils.save_json(data, self.cache_path)

        # caller get a list at the end
        return self

    def get(self, value, key=None):
        # not really efficient comapred to built-in SHUB collection. But we don't deal
        # with huge list so for now it will do.
        key = key or self.index
        return utils.search_list(self, key, value)

    def to_jl(self, filename=None):
        filename = filename or '{}.jl'.format(self.name)

        with open(filename, 'wb') as fd:
            for vessel in self:
                fd.write(json.dumps(vessel))

    def to_csv(self, filename=None):
        filename = filename or '{}.csv'.format(self.name)
        # the collection is expected to be generic, so infere data structure
        # from first item (caveat: we axpect all the objects to have the same
        # model)
        fieldnames = list(self[0].keys())

        with open(filename, 'wb') as fd:
            writer = csv.DictWriter(fd, delimiter=',', fieldnames=fieldnames)
            writer.writeheader()
            for vessel in self:
                # make arrays more human-friendly
                vessel['providers'] = ','.join(vessel.get('providers', []))
                vessel['_markets'] = ','.join(vessel.get('_markets', []))
                vessel['_env'] = ','.join(vessel.get('_env', []))

                writer.writerow(vessel)


def vessels(version='latest', disable_cache=None):
    """Encapsulate collection namings and initialization.

    Args:
        version(str): opt-in feature to control touchy release. We're expected
        to continupusly release the internal fleet with the `latest` version
        but if needed one can tweack it and limit who accesses a new static
        fleet. Useful for testing in staging or allowing rollbacks on risky changes.

    Returns:
        (Collection): Interface that exposes the same API as a list of vessels,
        with the following features:

            {
                _env": ["lpg_staging"],
                "_markets": ["lpg"],
                providers": ["EE", "VF API", "MT_API"],
                "name": "Hanne",
                "mmsi": null,
                "imo": "9712553",
                "call_sign": null
                "status": "Under Construction",
                "status_detail": "Launched",
            }

    """
    logger.debug('using `vessels-fleet` version: {}'.format(version))

    # beware: we don't check if it's `False` but `None`, hence not set by the user
    if disable_cache is None:
        # scraper containers on Scrapinghub are stateless, cache doesn't work
        disable_cache = settings.is_shub_env()

    coll = Collection(FLEET_S3_ARCHIVE.format(version=version), index='imo')
    return coll.load_and_cache(disable_cache)


# TODO support generic `**filters`
# TODO and no filter at all
def fetch_kpler_fleet(is_eligible, disable_cache=False):
    """Shortcut for functional filtering of the partial fleet we need.

    Args:
        is_eligible(Callable[Dict] -> bool): callback that decides if a
                                             vessel should be included in the list

    """

    return (vessel for vessel in vessels(disable_cache=disable_cache) if is_eligible(vessel))
