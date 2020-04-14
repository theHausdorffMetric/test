from base64 import b64encode
from datetime import datetime
import re

from kp_scrapers.lib.date import to_isoformat


TODAY_AS_ETA = ['IN BERTH', 'AT ANCHOR']


def is_future_eta(raw_eta):
    """Check the eta date is today onwards.

    Args:
        raw_eta (str):

    Returns:
        Boolean:

    """
    utc_now = datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat()
    eta = utc_now if raw_eta in TODAY_AS_ETA else to_isoformat(raw_eta)

    return True if eta >= utc_now else False


def _vessel_bargs(vessel_id):
    return b64encode(f'db=standard.vesselinfo&VID={vessel_id}'.encode()).decode()


def extract_vessel_imo(response):
    """Extract vessel imo from embedded vessel url

    Each entry of the vessel schdules table will show vessel name only.
    Vessel IMO is contained within an embedded html accessed via a link within each table entry.
    We want to extract vessel IMO for robustness of vessel matching.

    To do the above, we send a Request for each table entry to retrieve html.
    Then, we regex match for vessel IMO.

    Args:
        response (scrapy.HtmlResponse):

    Returns:
        str | None:

    """
    imo_match = re.match(r'.*"LRN":"(\d{7})"', response.body_as_unicode(), re.DOTALL)
    return imo_match.group(1) if imo_match else None
