from kp_scrapers.spiders.ais import safe_heading, safe_imo


SPIRE_AIS_SOURCES = {'satellite': 'S-AIS', 'terrestrial': 'T-AIS'}


def map_message_data(raw):
    # How the data was captured. Possible values: satellite or terrestrial
    # NOTE how does it compare to `source`?
    ais_type = SPIRE_AIS_SOURCES.get(raw['collection_type'])

    return {
        'aisType': ais_type,
        'message_type': raw.get('type'),
        # Vessel GPS geolocation accuracy in meters
        # Possible values: 1 (high, <=10 m), 0 (low, >10m, default)
        # unused: accuracy
        # ISO8601 formatted system ingestion time (time at which message is published) in UTC.
        # unused: created_at
        # unused: ais_version
        # Full NMEA 0183 v4 message
        # unused: nmea
        # Unique identifier for each message. Created by combining the timestamp and MMSI
        # unused: msg_id
        # unused: msg_description (should always be `position`)
        # it happens to sometimes be missing
        'master_name': raw.get('name'),
        'master_mmsi': str(raw['mmsi']),
        # Vessel country flag short code (derived from MMSI).
        'master_flag': raw.get('flag_short_code'),
        # unused: flag
        # unused: status
        # unused: length, width
        # unused: ship_type
        'master_shipType': raw.get('ship_and_cargo_type'),
        'master_callsign': raw.get('call_sign'),
        'master_imo': safe_imo(raw.get('imo')),
        'position_aisType': ais_type,
        # ISO8601 formatted timestamp (time at which message is transmitted) in UTC.
        'position_timeReceived': raw['timestamp'],
        # NOTE it's duplicated from the unused `position` field
        'position_lon': raw.get('longitude'),
        'position_lat': raw.get('latitude'),
        'position_course': raw.get('course'),
        'position_speed': raw.get('speed'),
        'position_heading': safe_heading(raw.get('heading')),
        # unused: rot
        # unused: maneuver
        'position_draught': raw.get('draught'),
        'nextDestination_destination': raw.get('destination'),
        'nextDestination_eta': raw.get('eta'),
    }


def map_vessel_data(raw):
    position = raw['last_known_position']
    destination = raw.get('most_recent_voyage', {})

    ais_type = SPIRE_AIS_SOURCES.get(position['collection_type'])

    # NOTE source? created_at? lifeboats
    # NOTE they use a uuid as well (`id`). We could create uuid only if not
    # already present
    return {
        # not used: 'created_at'
        'aisType': ais_type,
        # master data
        # from the `vessel` api
        # not used: 'class'
        # not used: 'lifeboats'
        # not used: 'individual_classification'
        # not used: 'general_classification'
        # not used: 'id'
        # not used: 'person_capacity'
        # not used: 'gross_tonnage'
        # not used: 'navigational_status'
        'master_name': raw['name'],
        'master_shipType': raw['ship_type'],
        'master_callsign': raw['call_sign'],
        # AIS item expects vessel ids to be str
        'master_imo': safe_imo(raw['imo']),
        'master_mmsi': str(raw['mmsi']),
        'master_flag': raw['flag'],
        'master_timeUpdated': raw['updated_at'],
        # FIXME actualy A+B
        # 'master_dimA': raw['length'],
        # FIXME actualy C+D
        # 'master_dimC': raw['width'],
        # not used: 'manoeuvrer'
        # not used: 'rot'
        # NOTE 'accuracy': Vessel GPS geolocation accuracy
        'position_lon': position['geometry']['coordinates'][0],
        'position_lat': position['geometry']['coordinates'][1],
        'position_aisType': ais_type,
        'position_course': position['course'],
        'position_draught': position['draught'],
        'position_heading': safe_heading(position['heading']),
        'position_speed': position['speed'],
        'position_timeReceived': position['timestamp'],
        # voyage
        'nextDestination_destination': destination.get('destination'),
        'nextDestination_eta': destination.get('eta'),
    }
