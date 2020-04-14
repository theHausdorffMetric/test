from datetime import datetime, timedelta
import re

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.spiders.port_authorities.barcelona import normalize


HEADER_MAPPING = ['ship_name', 'pier_Assigned', 'ship_type', 'date_arrival', 'date_departure']


def parse_listing_table(response):
    """Parse the listed results from the search.

    Args:
        scrapy.Response:

    Yield:
        Dict[str, Any]:

    """
    for row in response.xpath('//tr'):
        val = row.xpath('./td/span/@onclick')
        if val.get():
            _match = re.match(r'javascript:detalleBuque\(\'(?P<ship_id>[0-9]*)\'\)', val.get())
            if not _match:
                continue

            vessel_details = []
            vessel_details.append(row.xpath('./td/span//text()').extract_first())
            vessel_details.extend(
                [may_strip(val) for val in row.xpath('.//td/p//text()').extract() if may_strip(val)]
            )
            raw_item = dict(zip(HEADER_MAPPING, vessel_details))

            if _interpret_date(raw_item.get('date_arrival')):
                raw_item.update({'form_data': {'lloyds': _match.group('ship_id')}})
                yield raw_item
                # As per the source, the latest information is available at the top of the table,
                # we avoid procesing further rows once a the valid info with the date is obtained
                break


def parse_vessel_info(response):
    """Parse the vessel info.

    Args:
        scrapy.Response:

    Yield:
        Dict[str, Any]:

    """
    vessel_info = {}
    vessel_info.update(response.meta)
    for col in response.xpath('//span'):
        vessel_info[col.xpath('.//h6/text()').extract_first()] = col.xpath(
            './/p/text()'
        ).extract_first()
    yield normalize.process_item(vessel_info)


def _interpret_date(date):
    """Check if the record needs to be further parsed based on date

        Args:
            str

        Return
            Bool
        """
    try:
        date_to_test = datetime.strptime(date.strip(), '%d/%m/%Y %H:%M') - timedelta(hours=1)
    except ValueError:
        return False

    # As the source may contain information about very old port calls, we avoid processing
    # them by having a cutoff of 5 days. Any event older than 5 days frmo the date of scraping
    # will be ignored.
    if date_to_test >= datetime.utcnow() - timedelta(days=5):
        return True
    else:
        return False
