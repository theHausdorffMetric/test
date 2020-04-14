# -*- coding: utf-8; -*-

from __future__ import absolute_import

from kp_scrapers.business import AIS_HEADING_MISSING, IMO_MIN_LENGTH
from kp_scrapers.spiders.bases.markers import LngMarker, LpgMarker, OilMarker


AIS_TYPES = {'SAT': 'S-AIS', 'TER': 'T-AIS'}


# NOTE should be consistent with `charters`. Either create a local `utils.py`
# (explicit) or use indeed this file for untuitive imports (and API).
def safe_heading(risky_heading):
    """Make sure we store coherent heading values.

    Example:
        >>> safe_heading(234)
        234.0
        >>> safe_heading('234')
        234.0
        >>> safe_heading('234.0')
        234.0
        >>> safe_heading(None)
        >>> safe_heading('600')
        >>> safe_heading(4000)
        >>> safe_heading(511)

    """
    less_risky_heading = float(risky_heading) if risky_heading is not None else AIS_HEADING_MISSING

    if less_risky_heading == AIS_HEADING_MISSING or less_risky_heading >= 360:
        return None

    return less_risky_heading


def safe_imo(raw_imo):
    """Make sure the given imo looks like an imo (pretty naive).

    Some providers represent missing IMO with a `0` which can be an issue when
    propagating downstream. Hence we prefer to either transmit a real value or
    acknowledge the lack of it with an honest `None`.

    Example:
        >>> safe_imo(9363522)
        '9363522'
        >>> safe_imo(2324743)
        '2324743'
        >>> safe_imo(0)
        >>> safe_imo(1111)
        >>> safe_imo('M010119')
        >>> safe_imo('None')
        >>> safe_imo(None)

    """
    imo = str(raw_imo)
    return None if len(imo) < IMO_MIN_LENGTH or not imo.isdigit() else imo


# FIXME VesselTracker for example only covers LNG
class AisSpider(LngMarker, LpgMarker, OilMarker):

    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:ais']}

    @classmethod
    def category(cls):
        return 'ais'
