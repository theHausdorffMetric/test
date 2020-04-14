import datetime as dt

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import ISO8601_FORMAT


# cyclone forecasts use a few non-standard timezone abbreviations not found in pytz
# see https://www.nhc.noaa.gov/aboututc.shtml
NHC_TIMEZONE_OFFSET = {
    'Guam': 10,
    'HI': -10,
    'AK': -9,
    'PST': -8,
    'MST': -7,
    'CST': -6,
    'EST': -5,
    'AST': -4,
    # summer time
    'PDT': -7,
    'MDT': -6,
    'CDT': -5,
    'EDT': -4,
}


def find_active_blocks(el):
    return el.xpath("//document[@id = 'active']/folder[starts-with(@id, 'a')]")


def find_info_link(el, target):
    xpath_ = f"./networklink[@id = '{target}']/href/text()"
    return el.xpath(xpath_).extract_first()


def find_extended_data(el, attr_name):
    xpath_ = "./extendeddata/data[@name = '{}']/value/text()".format(attr_name)
    return el.xpath(xpath_).extract_first()


def find_forecasts(sel):
    for el in sel.xpath("//document/placemark"):
        xpath_ = "./polygon/outerboundaryis/linearring/coordinates/text()"
        coord = el.xpath(xpath_).extract_first()
        fcstpd = find_extended_data(el, 'fcstpd')

        if coord and fcstpd:
            yield (int(fcstpd), coord)


def find_forecast_point(sel, attr):
    xpath_ = f".//td[contains(text(), '{attr}')]/text()"
    return sel.xpath(xpath_).extract_first()


def parse_nhc_time(raw, fmt=ISO8601_FORMAT):
    """Convert a human-friendly date format into ISO DATE.

    Examples:
        >>> parse_nhc_time('2:00 PM AST August 26, 2019')
        '2019-08-26T18:00:00'
        >>> parse_nhc_time('2:00 PM CET August 26, 2019')  # central european time
        '2019-08-26T14:00:00'

    """
    # remove timezone string first, then offset for difference
    # TODO account for unknown tz strings
    offset = 0
    for tz in NHC_TIMEZONE_OFFSET:
        # whitespace prefix to avoid false positives
        if f' {tz}' in raw:
            raw = raw.replace(tz, '')
            offset = NHC_TIMEZONE_OFFSET[tz]
            break

    struct_dt = parse_date(raw) - dt.timedelta(hours=offset)
    return struct_dt.strftime(fmt)
