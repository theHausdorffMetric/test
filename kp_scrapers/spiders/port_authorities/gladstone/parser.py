from base64 import b64encode
import re


def _vessel_bargs(vessel_id):
    return b64encode(f'db=standard.vesselinfo&VID={vessel_id}'.encode()).decode()


def is_relevant_vessel(_type):
    # easy optimisation for discarding irrelevant vessels
    return _type in ['LIQUEFIED GAS TANKER']


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
