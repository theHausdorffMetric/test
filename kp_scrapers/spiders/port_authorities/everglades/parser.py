import logging
import re

from kp_scrapers.lib.utils import to_unicode


logger = logging.getLogger(__name__)


def extract_vessel_imo(response):
    """Extract vessel imo from embedded vessel url

    Each table entry of a port activity will show vessel name only.
    Vessel IMO is contained within an embedded html accessed via a link within each table entry.
    We want to extract vessel IMO for robustness of vessel matching.

    To do the above, we send a Request for each table entry to retrieve html.
    Then, we regex match for vessel IMO.

    Args:
        response (scrapy.HtmlResponse):

    Returns:
        str | None:

    """
    imo_match = re.match(r'.*"LRN":"(\d{6,7})', to_unicode(response.body), re.DOTALL)
    if not imo_match:
        logger.warning('Unable to extract vessel info, skipping')
        return

    return imo_match.group(1)
