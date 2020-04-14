import datetime as dt
import time

from scrapy.http import Request
from scrapy.selector import Selector
from six.moves.html_parser import HTMLParser

from kp_scrapers.lib.utils import unpack_kml
from kp_scrapers.models.enum import Enum
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.tropical_storm import TropicalStormSpider
from kp_scrapers.spiders.tropical_storm.jtwc import normalize
from kp_scrapers.spiders.tropical_storm.jtwc.parser import CycloneKMLParser


# NOTE sounds like something in utils/date.py
def utc_epoch(past_days=0):
    target_dt = dt.datetime.utcnow() - dt.timedelta(days=past_days)
    return int(time.mktime(target_dt.timetuple()) * 1000)


# supported doc type
# as defined on http://www.metoc.navy.mil/jtwc/jtwc.html
JTWCReportNames = Enum(
    tcfa='tcfa_doc.kml', tc='doc.kml'  # Tropical Cyclone Formation  # Tropical Depression
)


# TODO Inherit from an RSS parser
# NOTE there is a storm report as well: http://www.metoc.navy.mil/fwcn/fwcn.html#!/geospatial_kml.html  # noqa
class JtwcSpider(TropicalStormSpider):
    name = 'JTWC'
    version = '2.0.0'
    provider = 'JTWC'
    produces = [DataTypes.TropicalStorm]

    BASE_URL = 'https://www.metoc.navy.mil/jtwc/rss/jtwc.rss'
    reported_date = (
        dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
    )

    def __init__(self, past_days=0):
        epoch = str(utc_epoch(int(past_days)))
        self.start_urls = ['{base}?{date}'.format(base=self.BASE_URL, date=epoch)]

    def parse(self, response):
        for elt in response.xpath("//item/description").extract():
            ref = (
                Selector(text=HTMLParser().unescape(elt))
                .xpath("//li/a[text()[contains(., 'Google Earth')]]/@href")
                .extract()
            )

            # each cyclone has a different KMZ file describing it
            for url in ref:
                yield Request(url=url, callback=self.parse_warning_report)

    @staticmethod
    def select_parser(doc_name, sel):
        if doc_name == str(JTWCReportNames.tc):
            return CycloneKMLParser(sel)
        elif doc_name == str(JTWCReportNames.tcfa):
            # TODO discuss with analysts if they need formation alerts,
            # since they often evolve to become full-fledged cyclones within 24 hours
            #
            # example cyclone formation alert:
            #   - https://www.metoc.navy.mil/jtwc/products/wp9119.gif
            #   - https://www.metoc.navy.mil/jtwc/products/wp0619web.txt
            return None

        raise NotImplementedError(f"unknown doc name: {doc_name}")

    @unpack_kml
    def parse_warning_report(self, doc_name, sel, _):
        self.logger.info('parsing new cyclone alert: %s', doc_name)

        # NOTE we used to skip tropical cyclone but it seems it's better to
        # anticipate than being late
        doc = self.select_parser(doc_name, sel)
        if not doc:
            return

        winds_sustained, winds_gust, last_forecast_date = doc.winds()

        raw_item = {
            'name': doc.cyclone_name,
            'pretty_name': doc.pretty_name,
            'description': doc.description,
            'forecast_data': list(doc.forecast_track or []),
            'raw_report_date': doc.report_date,
            'raw_last_forecast_date': last_forecast_date,
            'raw_position': doc.position,
            'raw_forecast': doc.raw_forecast,
            'winds_sustained': winds_sustained,
            'winds_gust': winds_gust,
            'provider_name': self.provider,
            'reported_date': self.reported_date,
        }
        yield normalize.process_item(raw_item)
