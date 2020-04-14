# -*- coding: utf-8 -*-

"""Settings and constants for EE spider.
"""

from __future__ import absolute_import
import re


def strip_spaces(text):
    """Remove any kind of spaces from the given string."""
    return re.sub(r'\s+', '', text)


PROVIDER_ID = 'EE'

SERVICE_BASE_URL = 'https://services.exactearth.com/gws/wfs'

# this one is just for the sake of code readibility
WHATEVER_HIGH_LIMIT = 10000000
# just self-documented way to slice an array but return everything, i.e. `arr[:None]`
NO_FLEET_LIMIT = WHATEVER_HIGH_LIMIT

# calls try to get data from last request but we take a little margin
WINDOW_TOLERANCE = 3  # min

# XML response namespace
XMLNS = {
    'wfs': '{http://www.opengis.net/wfs/2.0}',
    'gml': '{http://www.opengis.net/gml}',
    # FIXME link this to `SERVICE_BASE_URL` (tricky because of `{`)
    'exactais': '{https://services.exactearth.com/gws}',
}

BAD_ETAS = ['00002460', '00000000', None]

# ExactAIS Filter helpers
FILTER_EQUAL = strip_spaces(
    """
<PropertyIsEqualTo>
    <PropertyName>{name}</PropertyName>
    <Literal>{value}</Literal>
</PropertyIsEqualTo>
"""
)

FILTER_WINDOW = strip_spaces(
    """
<PropertyIsGreaterThan>
    <PropertyName>{name}</PropertyName>
    <Function name="eeMaxAgoUTC">
        <Literal>{hours}</Literal>
    </Function>
</PropertyIsGreaterThan>
"""
).replace('Functionname', 'Function name')

FILTER_BETWEEN = strip_spaces(
    """
<PropertyIsBetween>
    <PropertyName>{name}</PropertyName>
    <LowerBoundary>
        <Literal>{lower}</Literal>
    </LowerBoundary>
    <UpperBoundary>
        <Literal>{upper}</Literal>
    </UpperBoundary>
</PropertyIsBetween>
"""
)
