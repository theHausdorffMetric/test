# -*- coding: utf-8 -*-

"""Marine Traffic Fleet synchronization
   ====================================

Fleet updates
~~~~~~~~~~~~~

We synchronize MarineTraffic fleet according to our static fleet data
downloaded from S3 (and generated currently by `data_egg` script on
`ct-pipeline`)

Basically this retrieves a fleet listing from Marine Traffic, compares
it to the content of the file and removes what is no longer in the egg
and adds what's in the ege but not in the listing.

Usage
~~~~~

**Sync**

To update the fleet you need to update the `vessel_data` static data first,
then you need to run this spider in "fleet update" mode. In a shell::

  $ scrapy crawl MarineTrafficFleet   \
        -a "fleet_name=<fleet_name>"  \
        -a getkey=<api key>           \
        -a setkey=<api key>           \
        -a imos=imo1,imo2             \
        -a dry_run=true               \
        -a removal=true

The `fleets` section below for the list of fleets we manage, also not
how the argument is quoted (because fleet names can include white spaces).

As a safety measure, deletion has been made optional: it requires an
argument to be passed to the spider for deletions to be actually
performed otherwise it simply lists what should be deleted.

`imos` flag allows to bypass sync from the static list and instead
force-register the given list.

Finally `dry_run`... dry runs the spider. No action is performed against
MarineTraffic fleet.

**Display**

One can retrieve the current list of vessels tracked just by passing
`read_only=true` to the spider, like so:

  $ scrapy crawl MarineTrafficFleet   \
        -a "fleet_name=<fleet_name>"  \
        -a getkey=<api key>           \
        -a read_only=true

"""

from __future__ import absolute_import, unicode_literals
import json

from kp_scrapers.lib.errors import InvalidCliRun
from kp_scrapers.lib.parser import serialize_response
from kp_scrapers.lib.static_data import fetch_kpler_fleet
from kp_scrapers.lib.utils import to_unicode
from kp_scrapers.spiders.ais.marinetraffic import api
from kp_scrapers.spiders.bases.markers import CoalMarker, CppMarker, LngMarker, LpgMarker, OilMarker
from kp_scrapers.spiders.fleet import AisFleetSpider, STATUS_DETAILS_ALLOWED


class MarineTrafficSpider(AisFleetSpider, LngMarker, LpgMarker, OilMarker, CppMarker, CoalMarker):
    """Synchronize our static fleet with MT fleets.

    Assumptions:

        - Vessels under construction are not tracked

    """

    name = 'MarineTrafficFleet'
    version = '1.0.2'
    provider = 'MT'

    spider_settings = {
        'DATADOG_CUSTOM_METRICS': [
            # we usually want to monitor the quantity of data found and the performance
            'item_scraped_count',
            'response_received_count',
            'fleet/active',
            'fleet/register',
            'fleet/remove',
            'fleet/fail',
            'fleet/garbage',
        ]
    }

    def __init__(
        self, fleet_name, getkey, setkey='', removal='', dry_run='', read_only='', imos=None
    ):
        # validate cli
        if fleet_name not in list(api.FLEETS.keys()):
            raise InvalidCliRun("fleet_name", fleet_name)
        elif not setkey and not read_only:
            self.logger.error("will try to sync fleet but no `setkey` was provided")
            raise InvalidCliRun("setkey", setkey)

        self.logger.debug("loading static vessels")
        # internal fleet is what we want to reflect on MT servers
        # load the vessels supposed to be tracked by MT
        if imos:
            # build the same layout of vessels as in our internal fleet list
            self.internal_fleet = [{'imo': imo, 'status': 'Active'} for imo in imos.split(',')]
        else:
            self.internal_fleet = list(fetch_kpler_fleet(lambda v: fleet_name in v['providers']))

        self.read_only = read_only.lower() == 'true'
        self.dry_run = dry_run.lower() == 'true'
        self._allow_removal = removal.lower() == 'true'

        self.client = api.MTClient(fleet_name, getkey=getkey, setkey=setkey)

        self.tags = ['fleet:{}'.format(fleet_name.lower())]

    def start_requests(self):
        cb = self.show_fleet if self.read_only else self.diff_fleet
        yield self.client.fetch_fleet(callback=cb)

    @serialize_response('jsono')
    @api.check_errors
    def show_fleet(self, fleet):
        # NOTE a better mapping could be useful (like using our `Vessel` item)
        # but since we have currently no use case, it is left as is
        for vessel in fleet['DATA']:
            yield vessel

    def on_feedback(self, response):
        # we want meta data so we can't use serialization decorator
        feedback = json.loads(response.body)
        # TODO handle error
        # TODO use a scrapy item
        yield {
            'success': 'success' in to_unicode(response.body.lower()),
            # FIXME [0]['detail'] fails with None doesn't have __getitem__
            # 'message': feedback.get('success', feedback.get('error')),
            'message': feedback,
            'imo': response.meta['imo'],
            'intention': response.meta['intention'],
        }

    def _inc_metric(self, metric_name_suffix):
        metric_name = 'fleet/{}'.format(metric_name_suffix)
        self.crawler.stats.inc_value(metric_name)

    @serialize_response('jsono')
    @api.check_errors
    def diff_fleet(self, fleet):
        """Looks at the fleet registered to find missing vessels

        If any missing vessel is found, as compare to the content of the vessel
        list in spiders_data, it is added to MarineTraffic's registry.

        A typicall response from MarineTraffic looks as follows (jsono protocol)::

            {
                'METADATA': {
                    'INACTIVE': 0,
                    'SATELLITE': 0,
                    'TERRESTRIAL': 7715
                }
                'DATA': [
                    {
                        'ACTIVE': '1',
                        'IMO': '9387554',
                        'MMSI': '228316600',
                        'SHIPNAME': 'KAOMBO SUL',
                        'SHIP_ID': '316'
                    }, ...
                ]
            }

        Marine Traffic language:

        - If the MMSI is blank (empty string), the vessel is UNKNOWN from
          MarineTraffic.
        - If it is prefixed with a '-' sign then it means the vessel is
          inactive (laid up, or even scrapped.).

        """
        self.logger.debug(
            'inspecting {} vessels from fleet {}'.format(
                fleet['METADATA']['TERRESTRIAL'], self.client.fleet_name
            )
        )
        self.logger.debug(
            'synchonizing {} vessels with MarineTraffic'.format(len(self.internal_fleet))
        )
        # TODO would be so much better to use a common metric and tag them with fleet
        self.crawler.stats.set_value('fleet/active', len(self.internal_fleet))

        internal_fleet_imos = set(
            [v['imo'] for v in self.internal_fleet if v['status_detail'] in STATUS_DETAILS_ALLOWED]
        )

        mt_valid_imos = set(
            [v['IMO'] for v in fleet['DATA'] if v['MMSI'] and not v['MMSI'].startswith('-')]
        )
        mt_inactive_imos = set([v['IMO'] for v in fleet['DATA'] if v['MMSI'].startswith('-')])
        mt_unknown_imos = set([v['IMO'] for v in fleet['DATA'] if not v['MMSI']])

        cant_register_imos = {
            imo: imo in internal_fleet_imos for imo in mt_inactive_imos.union(mt_unknown_imos)
        }
        to_register_imos = {
            imo: imo in internal_fleet_imos
            for imo in internal_fleet_imos.difference(mt_valid_imos)
            if imo not in cant_register_imos
        }

        return self.update_fleet(to_register_imos, cant_register_imos)

    def update_fleet(self, to_register_imos, cant_register_imos):
        for imo, wanted in to_register_imos.items():
            if wanted:
                self._inc_metric('register')
                self.logger.info('registering vessel #{}'.format(imo))
                if not self.dry_run:
                    # note that `imo` will be automatically added to the meta
                    # information passed to the callback
                    yield self.client.register(imo, self.on_feedback, intention='want_register')
            else:
                self.logger.warning('vessel #{} no longer required'.format(imo))
                if not self.dry_run and self._allow_removal:
                    self._inc_metric('remove')
                    yield self.client.remove(imo, self.on_feedback, intention='want_remove')

        for imo, wanted in cant_register_imos.items():
            if wanted:
                self.logger.warning('MT cannot track vessel #{}'.format(imo))
                if not self.dry_run and self._allow_removal:
                    self._inc_metric('fail')
                    yield self.client.remove(imo, self.on_feedback, intention='cant_register')
            else:
                self.logger.info('vessel #{} not tracked by MT but not wanted anyway'.format(imo))
                if not self.dry_run and self._allow_removal:
                    self._inc_metric('garbage')
                    yield self.client.remove(imo, self.on_feedback, intention='dont_care')
