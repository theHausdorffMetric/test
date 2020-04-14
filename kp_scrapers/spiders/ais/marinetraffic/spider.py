"""Marine Traffic AIS data collection
   ==================================

"""

from __future__ import absolute_import, unicode_literals

from scrapy.spiders import Spider
import six

from kp_scrapers.lib import utils
from kp_scrapers.lib.errors import InvalidCliRun
from kp_scrapers.lib.parser import serialize_response
from kp_scrapers.models.ais import AisMessage
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.ais import AIS_TYPES, AisSpider, safe_heading, safe_imo
from kp_scrapers.spiders.ais.marinetraffic import api


ais_key_map = {
    # /// Position
    'LON': ('position_lon', None),
    'LAT': ('position_lat', None),
    'SPEED': ('position_speed', lambda speed: (float(speed or 0.0) / 10.0)),
    # NOTE no `int`?
    'COURSE': ('position_course', None),
    # nothing to parse, it's already ISO8601 minus the timezone
    # NOTE could add timezone since it's in UTC
    'TIMESTAMP': ('reported_date', None),
    # NOTE why casting to unicode?
    'DRAUGHT': ('position_draught', lambda draught: six.text_type(float(draught or 0.0) / 10.0)),
    # NOTE only in simple?
    'STATUS': ('position_navState', None),
    'HEADING': ('position_heading', safe_heading),
    # /// ETA
    # NOTE 'CURRENT_PORT'?
    'DESTINATION': ('next_destination_destination', None),
    'ETA': ('next_destination_eta', lambda x: None if not x else x),
    # /// Master
    # NOTE Some messages will have an IMO number of 0 because
    # they describe inland tankers with only ENI registration.
    # In that case, set IMO value to be None.
    'IMO': ('master_imo', safe_imo),
    'MMSI': ('master_mmsi', None),
    'SHIPNAME': ('master_name', None),
    'SHIPTYPE': ('master_shipType', None),
    'CALLSIGN': ('master_callsign', None),
    'FLAG': ('master_flag', None),
    # NOTE LENGTH, WIDTH where is it on dimA, dimB, ...?
    # /// else
    # NOTE not sure if it's actually provided and why the default is terrestrial
    'DSRC': ('ais_type', lambda x: AIS_TYPES.get(x, 'T-AIS')),
    # not used: LENGTH, WIDTH, GRT, DWT, FLAG, YEAR_BUILT, LAST_PORT, LAST_PORT_TIME?
}


class MarineTrafficSpider(AisSpider, Spider):
    """MT API query to retrieve positions for our fleet managed by
    Marine Traffic.

    Doc: https://www.marinetraffic.com/en/ais-api-services/documentation/

    In the response the voyageData and positionData tags are optional, but at
    least one of them should be present.
    """

    name = 'MarineTrafficAIS2'
    version = '2.1.0'
    provider = 'MT_API'
    produces = [DataTypes.Ais, DataTypes.Vessel]

    # Ask for position received in the last TIMESPAN minutes.
    DEFAULT_TIMESPAN = 2
    # The default type of message to ask for.
    DEFAULT_MSGTYPE = 'simple'
    # NOTE I have no idea why
    DEFAULT_AIS_TYPE = 'T-AIS'

    def __init__(self, fleet_name, poskey, timespan=None, msgtype='simple'):
        if fleet_name not in list(api.FLEETS.keys()):
            raise InvalidCliRun("fleet_name", fleet_name)

        # NOTE this could benefit from being dynamic (dependent on jobs frequency)
        # this is currently achieved manually through cli args
        self._timespan = timespan or self.DEFAULT_TIMESPAN

        if msgtype in api.MESSAGE_TYPES:
            self._msgtype = msgtype
        else:
            self.logger.warning(
                'Unknown message type "{}", will use default instead: "{}"'.format(
                    msgtype, self.DEFAULT_MSGTYPE
                )
            )
            self._msgtype = self.DEFAULT_MSGTYPE

        self.client = api.MTClient(fleet_name, poskey=poskey)

    def start_requests(self):
        yield self.client.positions(self.parse_positions, self._timespan, self._msgtype)

    @serialize_response('jsono')
    @api.check_errors
    def parse_positions(self, positions):
        for ais_msg in positions:
            # validate and discard item if compulsory fields are not present
            if self._naive_validate(ais_msg):
                yield self.normalise_ais(ais_msg)

    @validate_item(AisMessage, normalize=True, strict=True, log_level='error')
    def normalise_ais(self, ais_msg):
        item = utils.map_keys(ais_msg, ais_key_map)

        item['position'] = {
            'ais_type': item.get('aisType', self.DEFAULT_AIS_TYPE),
            'course': item.pop('position_course', None),
            'draught': item.pop('position_draught', None),
            'heading': item.pop('position_heading', None),
            'lat': item.pop('position_lat', None),
            'lon': item.pop('position_lon', None),
            'nav_state': item.pop('position_navState', None),
            'received_time': item.get('reported_date'),
            'speed': item.pop('position_speed', None),
        }

        item['vessel'] = {
            'name': item.pop('master_name', None),
            'imo': item.pop('master_imo', None),
            'mmsi': item.pop('master_mmsi', None),
            'vessel_type': item.pop('master_shipType', None),
            'call_sign': item.pop('master_callsign', None),
            'flag_name': item.pop('master_flag', None),
        }

        item['ais_type'] = item.get('ais_type', self.DEFAULT_AIS_TYPE)

        if self._msgtype == 'extended':
            item['next_destination_ais_type'] = item['ais_type']

        item['provider_name'] = self.provider

        return item

    def _naive_validate(self, item):
        """Validate and discard item if compulsory fields are not present.

        NOTE fields are hardcoded, but it should be robust enough to prevent malformed items
             from being yielded and crashing downstream processes.

        TODO create a proper AIS model in `kp_scrapers.models`

        Args:
            item (Dict[str, str]):

        Returns:
            bool: False if item is invalid, else True

        """
        if not (item.get('IMO') or item.get('MMSI') or item.get('SHIPNAME')):
            self.logger.warning('AIS signal received is malformed: %s', item)
            return False

        return True
