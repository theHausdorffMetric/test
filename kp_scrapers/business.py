# -*- coding: utf-8 -*-

"""Kpler business knowledge."""

# Maritime knowledge ###

SHIPTYPE_MAP = {
    0: 'UNKNOWN',
    80: 'Tanker (Asphalt/Bitumen, Chemical, Crude Oil, Inland, Fruit Juice, Bunkering, Wine, Oil Products, Oil/Chemical, Water, Tank Barge, Edible Oil, Lpg/Chemical Tanker, Shuttle Tanker, Co2 Tanker)',  # noqa
    81: 'Tanker - Hazard A (?)',
    82: 'Tanker - Hazard B (?)',
    83: 'Tanker - Hazard C (?)',
    84: 'Tanker - Hazard D (Lng Tanker, Lpg Tanker, Gas Carrier)',
}

# as defined in AIS standard B
# https://www.navcen.uscg.gov/?pageName=AISMessagesB
# TODO many rules deserve their definition here and their handler in AIS spiders
AIS_HEADING_MISSING = 511.0
# we define a MIN length because one day we will reach `9999999` and grow.
IMO_MIN_LENGTH = 7

# oldest relevant vessel seen thus far:
# https://www.marinetraffic.com/en/ais/details/ships/shipid:429071
MIN_YEAR_SCRAPED = 1942

# smaller vessels than that don't bring anything to our platforms, as of today
MIN_INTERESTING_DWT = 10000
# we don't have upper limits on most platforms but above this threshold can be
# considered quite insanely big
MAX_INTERESTING_DWT = 500000

# vessel length range
MIN_INTERESTING_LENGTH = 0
MAX_INTERESTING_LENGTH = 500
