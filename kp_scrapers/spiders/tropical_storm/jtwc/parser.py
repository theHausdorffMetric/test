import datetime as dt
import logging
import re

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import to_unicode


logger = logging.getLogger(__name__)


class CycloneKMLParser(object):
    """Pretty API around KML document of cyclone report."""

    def __init__(self, response):
        self.sel = response

    def _x(self, xpath_def, **opts):
        return self.sel.xpath(xpath_def + '/text()').extract_first(**opts)

    @property
    def raw_forecast(self):
        polygon = self._x(
            "//placemark[contains(name, 'Swath')]/polygon/outerboundaryis/linearring/coordinates"
        )
        return polygon[1:] if polygon else polygon

    @property
    def description(self):
        return may_strip(self._x("//document/folder/description"))

    @property
    def pretty_name(self):
        # sometimes name returned can be like "HAGIBIS\n135\n135\n135"; remove trailing characters
        res = self._x("//document/folder/name")
        return may_strip(res.partition('\n')[0])

    @property
    def position(self):
        return self._x("//document/folder/folder/placemark/point/coordinates")

    @property
    def cyclone_name(self):
        """Extract friendly cyclone name.

        It should work with a string that looks like this:

            u'TROPICAL CYCLONE 15S (MARCUS) WARNING NR 23    '

        """
        match = re.search(r"(\w+) \(", to_unicode(self.description))
        if match:
            # it would be more accurate to use the report date (and it would
            # spare us the need to mock time while testing)
            this_year = dt.datetime.utcnow().year
            return '{}{}'.format(this_year, match.group(1).strip())
        else:
            logger.warning('unabled to parse cyclone name')

        return None

    @property
    def forecast_track(self):
        def _forecast_xpath(title):
            return f'./tr[td[contains(b, "{title}")]]/td/following-sibling::td[1]//text()'

        for table in self.sel.xpath("//placemark/description/table"):
            time = table.xpath(_forecast_xpath('TIME')).extract_first()
            position = table.xpath(_forecast_xpath('POSIT')).extract_first()
            wind_speed = table.xpath(_forecast_xpath('WIND')).extract_first()

            if time and position and wind_speed:
                yield {'date': time, 'wind': wind_speed, 'position': position}
            else:
                logger.warning('unable to parse forecast track data')

    @property
    def pre(self):
        return self._x("//document/folder/folder/placemark/description/pre", default='')

    @property
    def report_date(self):
        return re.search("\n(.*) POSIT:", self.pre).group(1)

    def winds(self):
        """Extract wind characteristics.

        We try to find the data through the description lines.
        Winds information should concern the several last ones and look like this:

        [
              '21/06Z, WINDS 125 KTS, GUSTS TO 150 KTS',
              '21/18Z, WINDS 135 KTS, GUSTS TO 165 KTS',
              '22/06Z, WINDS 130 KTS, GUSTS TO 160 KTS',
              '22/18Z, WINDS 125 KTS, GUSTS TO 150 KTS',
              '23/06Z, WINDS 115 KTS, GUSTS TO 140 KTS',
              '24/06Z, WINDS 080 KTS, GUSTS TO 100 KTS',
              '25/06Z, WINDS 040 KTS, GUSTS TO 050 KTS',
              '26/06Z, WINDS 025 KTS, GUSTS TO 035 KTS',
        ]

        """
        # initialize in case we don't find it
        winds_sustained, winds_gust, last_forecast_date = None, None, None

        for elt in self.pre.split('\n'):
            match = re.search(r'(\d{2}/\d{2}Z), WINDS (\d+) KTS, GUSTS TO (\d+)', elt)
            # we don't need to log `else` clause since half of the lines don't
            # contain the information we are searching for anyway
            if match:
                last_forecast_date = match.group(1)

                if not winds_gust and not winds_sustained:
                    winds_sustained = match.group(2)
                    winds_gust = match.group(3)

        return winds_sustained, winds_gust, last_forecast_date
