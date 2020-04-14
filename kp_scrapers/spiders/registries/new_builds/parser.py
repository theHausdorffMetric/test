import logging
import re

from kp_scrapers.lib.parser import may_strip, try_apply


logger = logging.getLogger(__name__)

# percentage threshold for DWT and length matching
APPROXIMATION = 0.05
# minimum DWT (in tons) for matching vessels
MIN_VESSEL_DWT = 10000


def is_vessel_too_small(vessel):
    """Check if vessel is too small.

    Args:
        vessel (Dict[str, str | int]):

    Returns:
        bool: True if vessel is below DWT limit, else False

    """
    return vessel.get('size_unit') == 'DWT' and vessel.get('size', 0) < MIN_VESSEL_DWT


def get_vessel_general_info(response):
    """Extract vessel general info from searching result list.

    Args:
        response (scrapy.Response):

    Returns:
        Dict[str, str]:

    """
    for tr_sel in response.xpath('//tbody//tr'):
        yield {
            'url': tr_sel.xpath('.//td[@class="v1"]//a/@href').extract_first(),
            'name': may_strip(''.join(tr_sel.xpath('.//td[@class="v2"]//a//text()').extract())),
            'build_year': tr_sel.xpath(
                './/td[@class="v3 is-hidden-mobile"]//text()'
            ).extract_first(),
            'dwt': tr_sel.xpath('.//td[@class="v5 is-hidden-mobile"]//text()').extract_first(),
        }


def extract_vessel_attributes(response):
    """Extract vessel info fvessel, response.meta['item'])rom vessel detail page.

    Args:
        response (scrapy.Response):

    Returns:
        Dict[str, str]:

    """
    raw_item = {}
    for pair in response.xpath('//tr'):
        key = pair.xpath('.//td[@class="n3"]//text()').extract_first()
        value = pair.xpath('.//td[@class="v3"]//text()').extract_first()

        if key and value:
            raw_item.update({key: value})
    raw_item.update(_extract_attr_from_url(response.url))
    return raw_item


def _extract_attr_from_url(url):
    """Extract vessel name, imo and mmsi from url.

    Args:
        url (str):

    Returns:
        Dict[str, str] | None:

    """
    _match = re.match(r'.*vessels/(\S+)-IMO-(\d+)-MMSI-(\d+)', url)
    if _match:
        name, imo, mmsi = _match.groups()
        return {
            'Vessel Name': name.replace('-', ' ').strip(),
            'IMO number': None if imo == '0' else imo,
            'MMSI': None if mmsi == '0' else mmsi,
        }
    else:
        logger.error(f'Vessel url pattern has changed: {url}')


def is_same_vessel(res, s3):
    """Compare if the search result and unknown build are the same vessel.

    Args:
        res (Dict[str, str]): search result from vessel finder
        s3 (Dict[str, str]): unknown build retrieved from s3

    Returns:
        Boolean:

    """
    # check if vessel name is the same (mandatory for all)
    vessel_name_1, vessel_name_2 = res.get('name'), s3.get('name')
    if not _is_equal(vessel_name_1, vessel_name_2):
        logger.debug(
            f'Vessel name is different: {vessel_name_1} from VesselFinder, {vessel_name_2} from S3'
        )
        return False

    # check if dwt is the same (optional with flag set to True)
    if s3.get('size_unit') == 'DWT':
        dwt_1, dwt_2 = res.get('dwt'), s3.get('size')
        is_same_dwt = _is_same_dwt(dwt_1, dwt_2)
        if not is_same_dwt:
            logger.debug(
                f'Dead weight is different: {dwt_1} from VesselFinder, {dwt_2} from S3. '
                f'Vessel name: {vessel_name_1}'
            )
            return False

    # check if build year is the same (Clarksons often has build years incorrectly often by one)
    build_year_1, build_year_2 = res.get('build_year'), s3.get('build_year')
    if not _is_same_year(build_year_1, build_year_2):
        logger.debug(
            f'Build year is different: {build_year_1} from VesselFinder, {build_year_2} from S3. '
            f'Vessel name: {vessel_name_1}'
        )
        return False

    logger.info(f'Found new build {vessel_name_1}, build year {build_year_1}')
    return True


def _is_equal(var_1, var_2):
    """Compare if two values are the same.

    Args:
        var_1 (str):
        var_2 (str):

    Returns:
        Boolean:

    """
    return may_strip(str(var_1)).upper() == may_strip(str(var_2)).upper()


def _is_same_year(year_1, year_2, offset=1):
    """Check if the build year is similar by approximation of ±1 year.

    We need the approximation because Clarksons often provides build year offset by one.

    Examples:
        >>> _is_same_year('2018', 2019)
        True
        >>> _is_same_year('2018', '2020')
        False
        >>> _is_same_year('-', 2019)
        False

    Args:
        dwt_1 (str):
        dwt_2 (int):

    Returns:
        Boolean:

    """
    if isinstance(year_1, str):
        year_1 = try_apply(may_strip(year_1), int)

    if isinstance(year_2, str):
        year_2 = try_apply(may_strip(year_2), int)

    if year_1 and year_2:
        _lower_bound, _upper_bound = year_1 - 1, year_1 + 1
        return _lower_bound <= year_2 <= _upper_bound

    return False


def _is_same_dwt(dwt_1, dwt_2):
    """Check if the dwt is similar by approximation of ±5%.

    Examples:
        >>> _is_same_dwt('45229', 44783)
        True
        >>> _is_same_dwt('-', 44000)
        False

    Args:
        dwt_1 (str):
        dwt_2 (int):

    Returns:
        Boolean:

    """
    if isinstance(dwt_1, str):
        dwt_1 = try_apply(may_strip(dwt_1), int)

    if isinstance(dwt_2, str):
        dwt_2 = try_apply(may_strip(dwt_2), int)

    if dwt_1 and dwt_2:
        _lower_bound, _upper_bound = dwt_1 * (1 - APPROXIMATION), dwt_1 * (1 + APPROXIMATION)
        return _lower_bound <= dwt_2 <= _upper_bound

    return False
