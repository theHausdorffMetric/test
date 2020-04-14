# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import socket

from lxml import etree
from pysimplesoap.client import SoapClient
from pysimplesoap.simplexml import SimpleXMLElement
from scrapy.exceptions import CloseSpider
from scrapy.http import Request
from scrapy.spiders import Spider
from six.moves.urllib import error

# VesselTracker contract ended on April 2018
from kp_scrapers.lib import static_data
from kp_scrapers.models.items import VesselPositionAndETA
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.ais import AisSpider, safe_heading
from kp_scrapers.spiders.bases.markers import DeprecatedMixin


MAINTENANCE_URL = 'http://webservice.vesseltracker.com/webservices/VesselListMaintenance'

# WARNING: in XML <?xml version="1.0" encoding="UTF-8"?> **must** be on the
# first line of the file/string
CONNECT_BODY = '''<?xml version="1.0" encoding="UTF-8"?>
<list><arg0>{login}</arg0>
<arg1>{password}</arg1></list>'''

VESSEL_QUERY_BODY = """<?xml version="1.0" encoding="UTF-8"?>
<{method}>
  <arg0>{login}</arg0>
  <arg1>{password}</arg1>{args}
</{method}>"""


# TODO: NCA: cut/pasted from etl.extraction.ais.vesselTrackerAPIPositions
# TODO use the one in utils. Implementation is slightly different so need to be tested
def extract_xpath_data(xpath):
    return xpath[0] if len(xpath) > 0 and len(xpath[0]) > 0 else None


class VesselTrackerSpider(DeprecatedMixin, AisSpider, Spider):
    """SOAP XML web-service query to retrieve positions for our fleet managed
    by Vessel Tracker.

    Sample response
    ---------------

       <?xml version="1.0" encoding="UTF-8"?>
       <S:Envelope xmlns:S="http://schemas.xmlsoap.org/soap/envelope/">
         <S:Body>
           <ns2:getMyVesselsResponse xmlns:ns2="http://webservice.vesseltracker.com/">
             <return timeCreated="2015-06-10T18:46:19.425+02:00">
               <vessel mmsi="605106030">
                 <masterData callsign="7TJC"
                      dimA="224" dimB="50" dimC="11" dimD="31"
                      imo="7400704" name="MOURAD DIDOUCHE" shipType="tankships"
                      timeUpdated="2015-06-04T22:59:08.568+02:00"/>
                 <voyageData datasource="VT" destination="ARZEW" draught="10.8"
                      eta="2015-06-10T08:00:00+02:00"
                      timeUpdated="2015-06-10T03:41:32.047+02:00"/>
                 <positionData course="206.0" datasource="VT" lat="36.26815"
                      lon="0.03751666666666666"
                      timeReceived="2015-06-10T03:54:40.778+02:00"/>
               </vessel>
             </return>
           </ns2:getMyVesselsResponse>
         </S:Body>
       </S:Envelope>

    In the response the voyageData and positionData tags are optional, but at
    least one of them should be present.
    """

    name = 'VesselTracker'
    provider = 'VT'
    version = '1.0.0'
    produces = [DataTypes.Ais, DataTypes.Vessel, DataTypes.PortCall]

    def __init__(
        self, fleet='', showfleet='', username=None, password=None, removal='', *args, **kwargs
    ):
        super(VesselTrackerSpider, self).__init__(*args, **kwargs)

        # TODO just remove default values
        # validate input params
        if any(x is None for x in (username, password)):
            raise CloseSpider('No credentials were provided !')

        self._user = username
        self._pass = password
        self._update_fleet = fleet.lower() == 'true'
        self._show_fleet = showfleet.lower() == 'true'
        self._allow_removal = removal.lower() == 'true'

        self.vessel_list = static_data.vessels()

        if self._show_fleet and self._update_fleet:
            self.logger.warning(
                'Contradictory arguments: cannot update and '
                'show fleet at once. Will assume command is '
                'show fleet.'
            )
            self._update_fleet = False

    def start_requests(self):
        """
        Notes
        -----

           start_requests is expected to return a Request and the framework
           doesn't check if it is, it simply uses the yielded object as if
           were we do what it expects, even if we do not care about the
           answer.

        """
        if self._update_fleet is True:
            for i in self.get_vessel_fleet():
                yield Request('http://www.google.com', meta={'info': i}, callback=self.info)
        if self._show_fleet is True:
            yield Request('http://www.google.com', callback=self.show_vessel_fleet)
        else:
            yield Request('http://www.google.com', callback=self.retrieve_positions)

    def get_client(self, uri):
        """Creates and returns a SOAP client to consume some API.
        """
        if not hasattr(self, '_client'):
            client = SoapClient(
                location=uri, action=uri, ns='ns3', namespace="http://webservice.vesseltracker.com/"
            )
            setattr(self, '_client', client)
        return self._client

    def call(self, uri, method, *args):
        '''Builds the a SOAP method call and runs it

        Args:
           uri (str): the SOAP method endpoint URI.
           method (str): the name of the method to call.
           args (tuple of str): extraneous arguments to pass to the method

        Note:
           Extraneous arguments should be passed in the same order as the
           remote method expects them.
        '''
        args = ''.join(
            [
                '<arg{argnum}>{value}</arg{argnum}>'.format(argnum=i + 2, value=value)
                for i, value in enumerate(args)
            ]
        )
        client = self.get_client(uri)
        client['AuthHeaderElement'] = {'username': self._user, 'password': self._pass}
        params = SimpleXMLElement(
            VESSEL_QUERY_BODY.format(
                password=self._pass, login=self._user, method=method, args=args
            )
        )
        return client.call(method, params)

    def retrieve_positions(self, _):
        """Calls the AIS position retrieval service and process the respone

        Note
        ----

           As explained in the :meth:`start_method` doc, this method is used
           as request callback even if we don't care for the respsonse from
           that request as everything meaningful is done within this method.
           This is just a trick to get the system bootstrapped.
        """
        try:
            response = self.call(
                'http://webservice.vesseltracker.com/webservices/SatExportService', 'getMyVessels'
            )
            try:
                for item in self._parse_response(response):
                    yield item
            except Exception as e:
                self.logger.error('Exception {}'.format(e.message))

        except (error.URLError, socket.timeout, socket.error) as exception:
            self.logger.warning(
                'Exception caught while calling VesselTracker'
                ' SOAP API: {}'.format(repr(exception))
            )

    def _parse_response(self, response):
        result = (etree.fromstring(response.getMyVesselsResponse.as_xml()).xpath('//return'))[0]
        for res in result.xpath('//vessel'):
            item = VesselPositionAndETA()

            next_destination_data = res.xpath('.//voyageData')
            if len(next_destination_data) > 0:
                next_destination_data = next_destination_data[0]
                item['nextDestination_destination'] = extract_xpath_data(
                    next_destination_data.xpath('@destination')
                )
                item['nextDestination_eta'] = extract_xpath_data(
                    next_destination_data.xpath('@eta')
                )
                item['nextDestination_timeUpdated'] = extract_xpath_data(
                    next_destination_data.xpath('@timeUpdated')
                )
                item['nextDestination_aisType'] = extract_xpath_data(
                    next_destination_data.xpath('@datasource')
                )
                item['position_draught'] = extract_xpath_data(
                    next_destination_data.xpath('@draught')
                )

                # From the point of view of our application positions
                # stores draught not port calls (or ETAs)
                item['position_draught'] = extract_xpath_data(
                    next_destination_data.xpath('@draught')
                )

            else:
                self.logger.info(
                    'received position without an ETA for vessel '
                    '{} (IMO: {}, MMSI: {})'.format(
                        item.get('master_name', '?'),
                        item.get('master_imo', '?'),
                        item.get('master_mmsi', '?'),
                    )
                )

            position_data = res.xpath('.//positionData')
            if len(position_data) > 0:
                position_data = position_data[0]
                item['position_aisType'] = extract_xpath_data(position_data.xpath('@datasource'))
                item['position_course'] = extract_xpath_data(position_data.xpath('@course'))
                item['position_heading'] = safe_heading(
                    extract_xpath_data(position_data.xpath('@heading'))
                )
                item['position_lat'] = extract_xpath_data(position_data.xpath('@lat'))
                item['position_lon'] = extract_xpath_data(position_data.xpath('@lon'))
                item['position_navState'] = extract_xpath_data(position_data.xpath('@navState'))
                item['position_speed'] = extract_xpath_data(position_data.xpath('@speed'))
                item['position_timeReceived'] = extract_xpath_data(
                    position_data.xpath('@timeReceived')
                )

            else:
                self.logger.info(
                    'VesselTracker: received an ETA without a '
                    'position for vessel {} (IMO: {}, MMSI: {})'.format(
                        item.get('master_name', '?'),
                        item.get('master_imo', '?'),
                        item.get('master_mmsi', '?'),
                    )
                )

            # Did we receive an ETA, a Position or both for this vessel in the
            # response
            if item.get('position_aisType') or item.get('nextDestination_destination'):
                master_data = res.xpath('.//masterData')[0]
                item['master_imo'] = extract_xpath_data(master_data.xpath('@imo'))
                item['master_mmsi'] = extract_xpath_data(res.xpath('@mmsi'))
                item['master_callsign'] = extract_xpath_data(master_data.xpath('@callsign'))
                item['master_dimA'] = extract_xpath_data(master_data.xpath('@dimA'))
                item['master_dimB'] = extract_xpath_data(master_data.xpath('@dimB'))
                item['master_dimC'] = extract_xpath_data(master_data.xpath('@dimC'))
                item['master_dimD'] = extract_xpath_data(master_data.xpath('@dimD'))
                item['master_name'] = extract_xpath_data(master_data.xpath('@name'))
                item['master_shipType'] = extract_xpath_data(master_data.xpath('@shipType'))
                item['provider_id'] = self.provider

                item['aisType'] = 'S-AIS & T-AIS'
                if 'nextDestination_aisType' not in item and 'position_aisType' in item:
                    item['aisType'] = item['position_aisType']
                elif 'nextDestination_aisType' in item and 'position_aisType' not in item:
                    item['aisType'] = item['nextDestination_aisType']
                elif item['position_aisType'] == item['nextDestination_aisType']:
                    item['aisType'] = item['position_aisType']
                else:
                    item['aisType'] = 'S-AIS & T-AIS'
                yield item
            else:
                del item

    ###
    #  Fleet management methods
    #

    def show_vessel_fleet(self, response):
        # retrieve list of our vessels managed by VT
        response = self.call(MAINTENANCE_URL, "list")
        from scrapy.selector import Selector

        vt_vessels = Selector(text=response.listResponse.as_xml(), type='xml').xpath('//return')
        # vt_vessels = (etree
        #               .fromstring(response.listResponse.as_xml())
        #               .xpath('//return'))
        fleet_imos = dict()
        fleet_mmsis = dict()
        for idx, vessel in enumerate(self.vessel_list):
            if vessel['imo'] in fleet_imos:
                self.logger.warning('duplicated IMO in fleet: {imo}'.format(**vessel))
            if vessel['mmsi'] in fleet_mmsis:
                self.logger.warning('duplicated MMSI in fleet: {mmsi}'.format(**vessel))
            fleet_imos.update({vessel['imo']: idx})
            fleet_mmsis.update({vessel['mmsi']: idx})

        for vessel in vt_vessels:
            d = {
                'name': vessel.xpath('./name/text()').extract_first(),
                'imo': vessel.xpath('./imo/text()').extract_first(),
                'mmsi': vessel.xpath('./mmsi/text()').extract_first(),
                'callsign': vessel.xpath('./callsign/text()').extract_first(),
                # 'shiptype': vessel.xpath('').extract_first(),
            }
            self.logger.info(
                'Vessel: {name}; IMO: {imo}; MMSI: {mmsi}; '
                'callsign: {callsign}'
                # 'ship type: {shiptype}'
                .format(**d)
            )
            yield d

    def get_vessel_fleet(self):
        # retrieve list of our vessels managed by VT
        response = self.call(MAINTENANCE_URL, 'list')
        vessels_vt = etree.fromstring(response.listResponse.as_xml()).xpath('//return/imo/text()')

        not_added_count = 0
        added_count = 0
        deleted_count = 0
        managed_vessel_count = 0
        lacks_imo_count = 0
        collected_imos = []
        collected_vessels = []
        for vessel in self.vessel_list:

            # Inactive vessels are already removed from vessels, but we also remove here
            # vessels which are at the beginning stages in Under Construction tp minimize
            # the number of API vessels to pay for
            if vessel['status_detail'] == 'In Build':
                continue

            if self.provider not in vessel['providers']:
                self.logger.info(
                    'Vessel {} ({}) not required to subscribe '
                    'to fleet {}'.format(vessel['name'], vessel['imo'], self.provider)
                )
                continue

            if not vessel['imo']:
                self.logger.warning(
                    'Cannot check if vessel {name} (MMSI: '
                    '{mmsi}) is managed by VT, it lacks its IMO'.format(**vessel)
                )
                not_added_count += 1
                lacks_imo_count += 1
                continue

            collected_imos.append(vessel['imo'])
            collected_vessels.append(vessel)

            if vessel['imo'] not in vessels_vt:
                # Add it to VT list
                self.logger.info(
                    'Adding Vessel {name} (IMO: {imo}, MMSI {mmsi}) to VT list'.format(**vessel)
                )

                add_response = self.call(MAINTENANCE_URL, 'add', vessel['imo'])
                add_result = etree.fromstring(add_response.addResponse.as_xml()).xpath(
                    '//return/@success'
                )[0]
                if add_result == 'true':
                    added_count += 1
                    self.logger.info(
                        "Vessel {name} (imo: {imo}) added to VT" " list".format(**vessel)
                    )
                else:
                    message = etree.fromstring(add_response.addResponse.as_xml()).xpath(
                        '//return/@message'
                    )[0]
                    self.logger.warning(
                        "Vessel {name} (IMO: {imo}, MMSI: "
                        "{mmsi}) could not be added to VT "
                        "list: {message}".format(message=message, **vessel)
                    )
            else:
                managed_vessel_count += 1
                self.logger.info(
                    'Vessel {name} (IMO: {imo}, MMSI: {mmsi}) '
                    'already managed by VT'.format(**vessel)
                )

        # Manage deletions
        to_remove = set(vessels_vt) - set(collected_imos)
        if to_remove:
            self.logger.info(
                'The following imos are to be deleted: {}'.format(', '.join(to_remove))
            )

        if self._allow_removal is not True:
            to_remove = set()  # We are not allowed to remove: empty the set.
            self.logger.info('Deletion not permitted will not proceed further.')

        else:
            # We use this for log messages, but no need to build it if we
            # are not to delete anything.
            vessel_dicts = {
                vessel['imo']: vessel
                for vessel in self.vessel_list
                if vessel not in collected_vessels
            }

        for imo in to_remove:
            del_response = self.call(MAINTENANCE_URL, 'remove', vessel['imo'])
            del_result = etree.fromstring(del_response.deleteResponse.as_xml()).xpath(
                '//return/@success'
            )[0]
            vessel = vessel_dicts.get(imo, {'imo': imo, 'name': 'unknown', 'mmsi': 'unknown'})
            if del_result == 'true':
                deleted_count += 1
                self.logger.info(
                    "Vessel {name} (imo: {imo}) removed from VT" " list".format(**vessel)
                )

            else:
                message = etree.fromstring(del_response.deleteResponse.as_xml()).xpath(
                    '//return/@message'
                )[0]
                self.logger.warning(
                    "Vessel {name} (IMO: {imo}, MMSI: "
                    "{mmsi}) could not be added to VT "
                    "list: {message}".format(message=message, **vessel)
                )

        yield {
            'vessel_count': len(self.vessel_list),
            'already_managed': managed_vessel_count,
            'added': added_count,
            'deleted': deleted_count,
            'not_added': not_added_count,
            'lacks_imo': lacks_imo_count,
        }
        self.logger.info(
            "On our list of {vessel_count} vessels:\n"
            " - {already} were already managed by VT;\n"
            " - {added} were added;\n"
            " - {deleted} were deleted;\n"
            " - {lacks_imo} lacks an IMO;\n".format(
                vessel_count=len(self.vessel_list),
                already=managed_vessel_count,
                added=added_count,
                deleted=deleted_count,
                not_added=not_added_count,
                lacks_imo=lacks_imo_count,
            )
        )
        if not_added_count:
            self.logger.warning("{} vessels not added to VT list".format(not_added_count))
        if lacks_imo_count:
            self.logger.warning("{} vessels lacks an imo.".format(lacks_imo_count))

    def info(self, response):
        # NCA: It could be nice to have an history/account of what's done by
        #      the fleet management jobs, but I don't know if we can
        #      distinguish
        # yield response.meta['info']
        pass
