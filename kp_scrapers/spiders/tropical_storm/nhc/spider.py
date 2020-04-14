import datetime as dt

from scrapy import Request, Selector

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.lib.utils import unpack_kml
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.tropical_storm import TropicalStormSpider
from kp_scrapers.spiders.tropical_storm.nhc import normalize, parser


class NhcSpider(TropicalStormSpider):
    """Scrape the National Hurricane center.

    The spider follows reports to build the final list of cyclones:

        - find all active cyclones
        - parse forecast cone
        - parse track

    """

    name = 'NHC'
    version = '2.0.0'
    provider = 'NHC'
    produces = [DataTypes.TropicalStorm]

    start_urls = ['https://www.nhc.noaa.gov/gis/kml/nhc_active.kml']
    reported_date = (
        dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
    )

    def parse(self, response):
        sel = Selector(text=response.body)

        # NOTE `find_active_blocks` finds only atlantic tropical cyclones
        actives = parser.find_active_blocks(sel)
        self.logger.info('step 1: found %s hurricane(s)', len(actives))
        for el in actives:
            cone = parser.find_info_link(el, 'cone')
            track = parser.find_info_link(el, 'track')

            if cone is None or track is None:
                self.logger.warning('Unable to find forecast cone and track')
                continue

            meta = {
                'pretty_name': parser.find_extended_data(el, 'tcName'),
                'name': parser.find_extended_data(el, 'atcfID'),
                'type': parser.find_extended_data(el, 'tcType'),
                'lat': parser.find_extended_data(el, 'centerLat'),
                'lon': parser.find_extended_data(el, 'centerLon'),
                'datetime': parser.find_extended_data(el, 'dateTime'),
                # 'wind' will be filled in later
                'cone': cone,
                'track': track,
            }

            yield Request(url=cone, callback=self.unpack_cone, meta=meta)

    @unpack_kml
    def unpack_cone(self, _, sel, meta):
        self.logger.info('step 2: parsing forecast')
        forecasts = list(parser.find_forecasts(sel))

        if forecasts:
            meta['cone'] = max(forecasts, key=lambda x: x[0])[1].strip()

            yield Request(url=meta['track'], callback=self.parse_track, meta=meta)
        else:
            self.logger.error('failed to parse forecast data, nothing to yield')

    @unpack_kml
    def parse_track(self, _, sel, meta):
        self.logger.info('step 3: parsing track')

        forecast_data = []
        position_origin = None
        (winds_gust, advisory_date, position_origin, raw_last_forecast_date) = (
            None,
            None,
            None,
            None,
        )

        # parse static data (vs historical data below)
        description = ' '.join([meta['type'], meta['name']])
        # NOTE why is it commented out?
        # advisory_date = parse_hnc_time(meta['datetime'])

        # get list of measurement points, ordered from current time to most distant forecast
        placemarks = [
            point
            for point in sel.xpath("//document/folder/placemark")
            if '<point>' in point.extract()
        ]
        self.logger.debug('walking cyclone {} datapoints'.format(len(placemarks)))

        for idx, el in enumerate(placemarks):
            # extract forecasted position, and corresponding wind speeds, e.g.
            #
            #   - '-72.1,31.9,0'
            #   - 'Valid at: 11:00 PM EDT August 27, 2019'
            #   - 'Maximum Wind: 35 knots (40 mph)'
            #   - 'Wind Gusts: 45 knots (50 mph)'
            #
            position = may_strip(el.xpath(".//coordinates/text()").extract_first())
            forecast_date = may_strip(parser.find_forecast_point(el, 'Valid at').partition(':')[2])

            winds_sustained = parser.find_forecast_point(el, 'Maximum Wind')
            winds_sustained = may_strip(winds_sustained.partition(':')[2].split('(')[0])
            winds_gust = parser.find_forecast_point(el, 'Wind Gusts')
            winds_gust = may_strip(winds_gust.partition(':')[2].split('(')[0])

            # first measurement point will always refer to the cyclone's current actual position
            if idx == 0:
                position_origin = position
                advisory_date = parser.parse_nhc_time(forecast_date)
                current_winds_sustained = winds_sustained
                current_winds_gust = winds_gust
                continue

            # build forecast cyclone track
            forecast_data.append(
                {
                    # in UTC time
                    'date': parser.parse_nhc_time(forecast_date),
                    # like other wind info, match with a regex
                    'wind': winds_sustained,
                    # lat and lon will be extracted after spliting around the comma
                    'position': position,
                }
            )

            # update our last date if we found a valid one
            raw_last_forecast_date = parser.parse_nhc_time(forecast_date)

        raw_item = {
            'name': meta['name'],
            'pretty_name': meta['pretty_name'].upper(),
            'description': may_strip(description),
            'forecast_data': forecast_data,
            'raw_report_date': advisory_date,
            'raw_last_forecast_date': raw_last_forecast_date,
            'raw_position': position_origin,
            # forecasted area of impact (>34 knots windspeed)
            'raw_forecast': meta['cone'],
            # winds at current cyclone position
            'winds_sustained': current_winds_sustained,
            'winds_gust': current_winds_gust,
            'provider_name': self.provider,
            'reported_date': self.reported_date,
        }
        yield normalize.process_item(raw_item)
