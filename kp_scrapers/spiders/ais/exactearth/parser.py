# -*- coding: utf-8 -*-

"""ExactAIS parsing logic.
"""

from __future__ import absolute_import
import datetime as dt
import logging

import dateutil.parser
from scrapy import Selector

from kp_scrapers.lib.parser import may_apply, may_strip
from kp_scrapers.models.ais import AisMessage
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.ais import safe_heading

from .constants import BAD_ETAS, PROVIDER_ID, XMLNS


logger = logging.getLogger(__name__)


def extract(tree, node_name, default=None):
    """Abstract away property access from EE XML responses.

    :param tree: fn - XML tree selector function
    :param node: str - node name in XML tree

    :return: str - node value if found
    """
    # scrapy xpath doesn't work with xml namespaces as easy as plain xml path
    xml_path = '{exactais}{node}'.format(node=node_name, **XMLNS)
    node = tree.find(xml_path)
    return node.text if node is not None else default


# TODO move it in dates if this format is generic ?
def parse_eta_fmt(updated_at_year, raw_eta):
    """Parse the cryptic int specific to ETA: .

    Args:
        updated_at_year(int): ETA format doesn't provide the year, so we do
        raw_eta(int): MMddhhmm as received from EE: MMDDHHmm

    Returns:
        (datetime.datetime): equivalent datetime

    Raises:
        (ValueError): in case we got something impossible (ex: month 24)
    """
    # TODO handle year change (check months)
    # FIXME many cases not handled:
    #   - 00002445
    # most parsing libraries expect max hours and minutes to be 23 and 59
    # eta messages use it a lot
    raw_eta = raw_eta.replace('2460', '2359')
    return dt.datetime.strptime(raw_eta, '%m%d%H%M').replace(year=updated_at_year)


@validate_item(AisMessage, normalize=True, strict=True, log_level='error')
def _parse_node(url, node):
    """Parse a single vessel XML structure.

    Args:
        url(str): original request url
        node(xml.ElementTree): partial of the initial XML response

    Returns:
        (kp_scrapers.models.items.VesselPosition): Structured information of the vessel
    """
    ais_type = extract(node, 'source')

    raw_pos_updated_at = extract(node, 'dt_pos_utc')
    pos_updated_at = None
    if raw_pos_updated_at:
        pos_updated_at = dateutil.parser.parse(raw_pos_updated_at).isoformat()

    static_updated_at = None
    raw_static_updated_at = extract(node, 'dt_static_utc')
    if raw_static_updated_at:
        static_updated_at = dateutil.parser.parse(raw_static_updated_at).isoformat()

    try:
        raw_eta = extract(node, 'eta')
        eta = parse_eta_fmt(dt.datetime.utcnow().year, raw_eta).isoformat() if raw_eta else None
    except (TypeError, ValueError, AttributeError) as e:
        if raw_eta not in BAD_ETAS:
            # those values is probably the None equivalent of the ais emmitter...
            # so since this case is known, we only log the others.
            # some of them are not supported and others are simply bad formatted
            logger.debug('unable to parse eta: {} ({})'.format(e, raw_eta))
        eta = None

    imo = extract(node, 'imo')
    item = {
        'vessel': {
            'name': may_strip(extract(node, 'vessel_name')),
            'imo': None if imo == '0' else imo,
            'mmsi': extract(node, 'mmsi'),
            'vessel_type': extract(node, 'vessel_type_code'),
            'call_sign': extract(node, 'callsign'),
        },
        'position': {
            'lat': may_apply(extract(node, 'latitude'), float),
            'lon': may_apply(extract(node, 'longitude'), float),
            'speed': may_apply(extract(node, 'sog'), float),
            'course': may_apply(extract(node, 'cog'), float),
            'ais_type': ais_type,
            'received_time': pos_updated_at,
            'heading': safe_heading(extract(node, 'heading')),
            'nav_state': may_apply(extract(node, 'nav_status_code'), int),
            # current draught values proved to be outwright wrong or late on the
            # platform, messing up with a lot of our features. It needs more
            # investigation but at this moment we need to stop it, although still
            # receive data from EE to continue assessing its quality
            'draught_raw': may_apply(extract(node, 'draught'), float),
        },
        'reported_date': static_updated_at,
        'provider_name': PROVIDER_ID,
        'ais_type': ais_type,
        'message_type': extract(node, 'message_type'),
        'next_destination_eta': eta,
        'next_destination_ais_type': ais_type,
        'next_destination_destination': may_strip(extract(node, 'destination')),
    }

    return item


def parse_response(response):
    """Parse raw XML API response.

    The method tries its best to iterate over a set of items without crashing,
    since a bad item shouldn't prevent the scraper to aggregate the others.
    """
    tree = Selector(response).root

    for node in tree.findall('{gml}featureMembers/'.format(**XMLNS)):
        try:
            yield _parse_node(response.url, node)
        except Exception as e:
            logger.warning('failed to parse node: %s', e)
            # unfortunately it happens and then should be fixed, but it
            # shouldn't prevent the other items to be scrapped
            continue
