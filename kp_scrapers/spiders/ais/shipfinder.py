# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, unicode_literals
from datetime import datetime, timedelta
import json

from scrapy import settings
from scrapy.http import Request
from scrapy.spiders import Spider
import six
from six.moves import range

from kp_scrapers.lib import static_data
from kp_scrapers.lib.date import EPOCH_WITH_UTC_TZ
from kp_scrapers.models.items import VesselPositionAndETA
from kp_scrapers.spiders.ais import AisSpider, safe_heading
from kp_scrapers.spiders.bases.markers import DeprecatedMixin


# NOTE should we move it to lib/date ?
def this_year_or_the_next(month, reference_date=datetime.utcnow()):
    """Given a reference date and a month tells if that month refers to the
    month from the current year or the next.

    If the given month has already elapsed during at the time of the reference
    date, then it is assumed that month refers to that same month, but the next
    year. Otherwise it is assumed that month is the month of the same year as
    the year of ``reference_date``.

    This function is used for data source that provide relative/incomplete
    dates (without an explicit year).

    TODO
    ----

       Known code duplication: this code was copied from
       The function there is named :func:`etl.utils.convert.guess_year`

    Examples
    --------

    >>> this_year_or_the_next(1, reference_date=datetime(1981, 6, 1, 0, 0))
    1982
    >>> this_year_or_the_next(1, reference_date=datetime(1981, 1, 1, 0, 0))
    1981
    >>> this_year_or_the_next(1, reference_date=datetime(1981, 1, 31, 0, 0))
    1981
    >>> this_year_or_the_next(1, reference_date=datetime(1981, 2, 1, 0, 0))
    1982
    """
    if month >= reference_date.month:
        return reference_date.year
    else:
        return reference_date.year + 1


class ShipFinderSpider(DeprecatedMixin, AisSpider, Spider):
    """Collects AIS data from ShipFinder.com through their REST API.

    !! deprecation notice !! produciton testing reveals poor quality of data
    (bad positions as well as time lag), hence we decided to no pursuie a
    contract

    """

    name = 'ShipFinderAPI'

    URL = 'http://api.shipxy.com/apdll/ap.dll?v=1&k={api_key}&enc=1&cmd=2003&id={mmsi_id}'
    API_STATUS = {
        0: 'success',
        1: 'False, unknown reason',
        3: 'This ship does not exist',
        6: 'The duration of the key is expired',
        7: 'The key is blocked',
        9: 'The key does not exists',
        12: 'Too much ship in one time, refuse',
        13: 'The server is busy now',
        14: 'Your domain name is illegal',
        15: 'The number of ship exceed your quota',
        16: 'This ship is out of your registered zone',
        17: 'Wrong key',
        100: 'Parameter error',
        1000: 'False to connect with server',
        -1: 'No status in json',
    }
    PROVIDER_SHORTNAME = 'SF'

    MMSI_DIGIT_COUNT = 9  # Number of digit in an MMSI id.
    URL_MAX_SIZE = 2078

    def start_requests(self):
        # TODO use cli argument instead
        SF_API_KEY = '0FFD52AC4B28052D83156C053AFE4CD0'
        # ovewrite api key if explicitely set
        if hasattr(settings, 'SF_API_KEY'):
            SF_API_KEY = getattr(settings, 'SF_API_KEY')

        vessel_list = static_data.vessels()

        # There is a limit to size of the URL request. If the URL size is
        # above, a 404 Error is returned.
        # We compute the maximum number of MMSI we can send at once given
        # the length of the URL pattern and the elements to be inserted in
        # it (the +1 is for the ',' inserted in between the MMSIs we send).
        batch_size = (self.URL_MAX_SIZE - len(self.URL) - len(SF_API_KEY)) // (
            self.MMSI_DIGIT_COUNT + 1
        )

        # Then we send as many requests as needed.
        for bound in range(0, len(vessel_list), batch_size):
            # We build the vessels_by_mmsi dictionary inside the loop because
            # it will be mutated within the response parsing function.
            # Sharing it between requests is thus a bad idea.
            vessels_by_mmsi = {
                v['mmsi']: v
                for v in vessel_list[bound : bound + batch_size]
                if self.PROVIDER_SHORTNAME in v.get('providers', []) and v.get('mmsi')
            }
            batch = ','.join(list(vessels_by_mmsi.keys()))
            yield Request(
                self.URL.format(api_key=SF_API_KEY, mmsi_id=batch),
                callback=self.parse,
                meta={'vessels': vessels_by_mmsi},
            )

    def parse(self, response):
        '''Parses the json response returned by MarineTraffic's API.


        Sample Response
        ---------------

        Here is what are repsonse from the MarineTraffic API looks like
        (indented for the sake of readability).

           {"status":0,
            "data":[
             {"ShipID":477744900,
              "From":0,
              "mmsi":477744900,
              "shiptype":80,
              "imo":9415703,
              "name":"KIKYO",
              "callsign":"VRGR7",
              "length":2250,
              "width":360,
              "left":150,
              "trail":380,
              "draught":11800,
              "dest":"IND_VIZAG",
              "eta":"06-20 18:00",  //ETA: Time format : “MM-DD HH-MM”
              "navistat":0,
              "lat":5702933,
              "lon":80408952,
              "sog":8694,
              "cog":9020,
              "hdg":9100,
              "rot":0,
              "lasttime":1434610919
              }
             ]
            }

        '''
        vessels_by_mmsi = response.meta['vessels']

        if 200 == response.status:
            # TODO: Why the replace() ? JSON received looks properly
            #       formatted (historical artifact ?)
            json_response = json.loads(response.body.replace(r"\'", "'").replace(",]", "]"))

            if json_response.get('status', -1) != 0:
                s = json_response['status']
                self.logger.error(
                    'Bad response from shipfinder. Return code'
                    ' {}: {}'.format(s, self.API_STATUS.get(s, 'Unknown'))
                )
            else:
                for item in self._do_parse(json_response, vessels_by_mmsi):
                    yield item

        else:
            self.logger.error('Bad HTTP response from shipfinder API: {}'.format(response.body))

    def _do_parse(self, json_response, vessels_by_mmsi):
        """Parses thes the list of vessel updates returned by the API.

        Args:
            json_response (dict): a python dict representing the JSON
            vessel_list_by_mmsi (dict): a dict containing our list of vessels


        """
        for json_object in json_response.get('data', []):
            try:
                current_vessel = vessels_by_mmsi.pop(six.text_type(json_object['mmsi']))
            except KeyError:
                self.logger.error(
                    'Vessel with mmsi {} has been returned two'
                    ' times by ShipFinder API.'.format(json_object['mmsi'])
                )
                current_vessel = None
                # NCA: shall we go on or use 'continue' to skip this response ?

            # Sometimes ShipFinder returns weird response we use this to weed
            # them out (see ticket #1488).
            if (
                2147483647 == json_object.get('lat')
                and 2147483647 == json_object.get('lon')
                and 0 == json_object.get('lasttime')
            ):
                self.logger.warning(
                    'Received strange position for Vessel with'
                    'MMSI {}, skiping it ({})'.format(
                        current_vessel or 'Unknown', json.dumps(json_object)
                    )
                )
                continue

            res = {
                'master_name': json_object.get('name', ''),
                'master_imo': six.text_type(json_object.get('imo', '')),
                'master_mmsi': six.text_type(json_object.get('mmsi', '')),
                'master_callsign': json_object.get('callsign', ''),
            }
            # Is this condition realistic ?
            if 'lasttime' in json_object:
                # TODO: BUG?: NCA: Is this correct ?
                #                  What is the reference here ?
                #
                #   pytz.utc.localize(
                #       datetime.fromtimestamp(
                #           json_object['lasttime'])).isoformat()
                #
                # Shouldn't this read something like:
                #
                #   (dates.EPOCH_WITH_UTC_TZ
                #    + timedelta(seconds=int(json_object['lasttime'])))
                #

                # The real question being: is the timestamp in UTC or in
                # local time (and if local, local from
                # where [ship finder is a japanese company]).
                # According to:
                #   - my tests: the timestamp is in UTC and
                #   - Python's Doc: the conversion above is sensitive to the
                #     local clock timezone (underneath, localtime() is
                #     called). Thus I replaced it with the current code.
                #
                # Which means if someone screws the ETL machine clock
                # date loaded in the database are still OK.
                #
                position_timestamp = EPOCH_WITH_UTC_TZ + timedelta(seconds=json_object['lasttime'])
                res.update(
                    {
                        'position_aisType': 'T-AIS',
                        'position_course': json_object['cog'] / 100.0,
                        'position_draught': json_object['draught'] / 1000.0,
                        'position_heading': safe_heading(json_object['hdg'] / 100.0),
                        'position_lat': float(json_object['lat']) / 1000000.0,
                        'position_lon': float(json_object['lon']) / 1000000.0,
                        'position_navState': json_object['navistat'],
                        'position_speed': json_object['sog'] / 514.444444,
                        'position_timeReceived': position_timestamp.isoformat(),
                        'aisType': 'T-AIS',
                        'provider_id': self.PROVIDER_SHORTNAME,
                    }
                )
            else:
                self.logger.warning(
                    'ShipFinderAPI response: {}, has no '
                    'lasttime for {}.'.format(json_object, current_vessel)
                )
            if json_object.get('eta', "") != "" and json_object.get('dest', "") != "":
                try:
                    res.update(
                        {
                            'nextDestination_destination': json_object['dest'],
                            # raw Time format from SF is the AIS format : “MM-DD HH-MM”
                            'nextDestination_eta': datetime(
                                this_year_or_the_next(
                                    min(int(json_object['eta'][:2]), 12),
                                    reference_date=position_timestamp,
                                ),
                                min(int(json_object['eta'][:2]), 12),
                                min(int(json_object['eta'][3:5]), 31),
                                min(int(json_object['eta'][6:8]), 23),
                                min(int(json_object['eta'][9:11]), 59),
                            ).isoformat(),
                        }
                    )
                except ValueError:
                    # If eta "02-30 00:28" contains not real date, for example
                    # 30th February skip item
                    self.logger.warning(
                        "ShipFinder's response for vessel {} "
                        "contains eta {} that can not be parsed".format(
                            json_object['imo'], json_object['eta']
                        )
                    )
                    continue
            else:
                self.logger.warning(
                    "ShipFinder's response for vessel {} "
                    "contains no ETA.".format(json_object['imo'])
                )

            item = VesselPositionAndETA(**res)
            # item['originalJson'] = json.dumps(res)
            yield item

        # Checks if we received an answer for every vessels we requested.
        if vessels_by_mmsi:
            missing_imos = ','.join(v['imo'] for v in vessels_by_mmsi.values())
            self.logger.warning(
                'ShipFinder API did not return data for the '
                'vessels with the following IMOs: {}'.format(missing_imos)
            )
