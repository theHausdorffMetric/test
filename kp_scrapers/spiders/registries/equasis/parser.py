from functools import partial
import logging
import re

import dateutil.parser
from six.moves import map

from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.items import Vessel
from kp_scrapers.models.utils import validate_item
from kp_scrapers.models.vessel import VesselRegistry


logger = logging.getLogger(__name__)


def parse_date(raw, exclude=None):
    """Parse a raw string date and returns the equivalent isoformat.
    Before parsing, any element of exclude will be removed from the raw string.
    """
    if not raw:
        return ''
    for token in exclude or []:
        raw = raw.replace(token, '')
    return dateutil.parser.parse(raw.strip(), dayfirst=True).isoformat()


# define specs for mapping expected item fields with the ones found on the web page
COMPANY = {
    'IMO number': ('imo', str),
    'Date of effect': (
        'date_of_effect',
        partial(parse_date, exclude=['since ', 'during ', 'before ']),
    ),
    # NOTE the way 'before' is parsed is really not accurate
    'Role': ('role', str),
    'Address': ('address', str),
    'Name of company': ('name', str),
}

VESSEL_FIELDS = {
    'Year of build': ('build_year', int),
    'Call Sign': ('call_sign', lambda x: None if 'unknown' in x.lower() else x),
    'DWT': ('dead_weight', int),
    'Flag': ('flag_name', lambda x: x.replace('(', '').replace(')', '')),
    'Gross tonnage': ('gross_tonnage', int),
    'imo': ('imo', str),
    'MMSI': ('mmsi', str),
    'name': ('name', str),
    'Status': ('status', str),
    # TODO not required in VesselRegistry model, clarify with analysts on criticality
    # 'status_date': ('status_date', partial(parse_date, exclude=['(during ', '(since ', ')'])),
    'Type of ship': ('type', str),
    'updated_time': ('reported_date', parse_date),
}

SAFETY_CERTIFICATES = {
    'Classification society': ('classification_society', str),
    'Date survey': ('survey_date', parse_date),
    'Date expiry': ('expiry_date', parse_date),
    'Date change status': ('status_change_date', parse_date),
    'Status': ('status', str),
    'Reason': ('reason', str),
    'Top C/V': ('CV', str),
}

# Names of classification sections
STATUS_SECTION = 'Status'
SURVEYS_SECTION = 'Surveys'


@validate_item(VesselRegistry, normalize=True, strict=True)
def parse_vessel_details(selector, provider='Equasis', whitelist=None, blacklist=None):
    # initialise with base vessel properties
    item = _parse_base_vessel(selector)

    # append management data to vessel
    item.update(
        companies=_parse_table(selector.css('#collapse3 .tableLS'), mapper=COMPANY),
        # TODO not sure if valuable, almost no Equasis vessels carry this info
        # safety_certificates=_parse_table(
        #     selector.css('#collapse5 .tableLS'),
        #     mapper=SAFETY_CERTIFICATES,
        # ),
        **_parse_classification(selector.css('#collapse4 .access-body')),
    )

    # append meta info
    item.update(provider_name=provider)

    # trim item based on whitelist/blacklist
    if whitelist:
        res = {field: item[field] for field in whitelist}
    elif blacklist:
        [item.pop(field, None) for field in blacklist]
        res = item
    # no whitelist/blacklist specified
    else:
        res = item

    return res


def _parse_base_vessel(selector):
    # Extract the raw data
    table = {}
    for row in selector.css('.access-item .row'):
        raw_cells = [x for x in row.css('*::text').extract() if x.strip()]
        # NOTE replace also '()' ? (cf flag, status_update)
        cells = [y.strip() for y in raw_cells]
        if len(cells) > 1:
            field_name = cells[0]
            table[field_name] = cells[1]
            if field_name == 'Status' and len(cells) > 2:
                table['status_date'] = cells[2]

    # extract name and imo
    cells = selector.css('.info-details h4 b *::text').extract()
    if len(cells) >= 2:
        # otherwise no imo, should we crash and skip the item ?
        table['name'] = cells[0]
        table['imo'] = cells[1]

    # extract last updated at
    cells = selector.css('.info-details .badge *::text').extract()
    if cells:
        cells = cells[0].split()
        if len(cells) >= 3:
            table['updated_time'] = cells[2]

    if not len(table):
        raise ValueError('unable to parse the page, no content found')

    # fit the data in the expected model
    vessel = Vessel(map_keys(table, VESSEL_FIELDS))

    return vessel


def _parse_table(selector, mapper):
    result = []
    if not selector:
        return result

    titles = [title.strip() for title in selector.css('thead th *::text').extract()]
    for line in selector.css('tbody tr'):
        cells = [''.join(td.css('*::text').extract()) for td in line.css('td')]
        res = map_keys({titles[i]: cell.strip() for i, cell in enumerate(cells)}, mapper)
        result.append(res)
    return result


def _parse_classification(selector):
    all_status = []
    all_surveys = []
    current_section = None

    # All the classification rows have similar classes and attributes. To distinguish them we look
    # for headers (STATUS_SECTION, SURVEYS_SECTION) and parse subsequent rows according to which
    # section we are in
    for row in selector:
        text_fields = _extract_text_fields(row)
        if len(text_fields) == 1:
            current_section = text_fields[0]
        elif current_section == STATUS_SECTION:
            # classification_status
            status = {}
            if len(text_fields) > 0:
                status['classification_society'] = text_fields[0]
            if len(text_fields) > 1:
                status['status'] = text_fields[1]
            if len(text_fields) > 2:
                status['status_change_date'] = parse_date(
                    text_fields[2], exclude=['since ', 'during ', 'before ']
                )
            if status:
                all_status.append(status)

        elif current_section == SURVEYS_SECTION:
            # classification_surveys
            survey = {}
            if len(text_fields) > 0:
                survey['classification_society'] = text_fields[0]
            if len(text_fields) > 2:
                survey['last_renewal_date'] = parse_date(text_fields[2])
            if len(text_fields) > 4:
                survey['next_renewal_date'] = parse_date(text_fields[4])
            if survey:
                survey['details_url'] = row.css('a::attr(href)').extract_first()
                all_surveys.append(survey)
        else:
            logger.debug('skipped a classification row, content={}'.format(text_fields))

    return {'classification_statuses': all_status, 'classification_surveys': all_surveys}


def _extract_text_fields(selector):
    """Generates a list of non empty text fields inside of a selector.
    Fields with only spacings are considered empty.
    """
    fields = selector.xpath('descendant::*/text()').extract()
    return [x for x in [f.strip() for f in fields] if x]


def parse_imos_from_search_results(selector):
    """Given a search page, we extract all the links to vessel pages and we get the IMOs from
    those links.
    It is possible to get the same IMO multiple times since the Equasis website hides and shows
    cells based on the screen size. So we turn the list into a set to get rid of duplicates.
    """
    return set(map(_parse_imo_from_link, _parse_vessel_links_from_search(selector)))


def _parse_imo_from_link(link):
    return re.search('(\d+)', ''.join(link.css('*::attr(onclick)').extract())).groups()[0]


def _parse_vessel_links_from_search(selector):
    return selector.css('form[name=formShip] table a')


def parse_number_of_results(selector):
    results = selector.xpath("//div[@class='form-group results']/p/strong/text()").extract()
    return [int(count) for count in results]


def parse_page_links(selector):
    """Wrap css logic to target results pages list."""
    # take care of case where there is no pagination at all
    if not selector.css('.pagination'):
        return []

    try:
        return [s.strip() for s in (selector.css('.pagination')[0].css('li>a *::text').extract())]
    except Exception as err:
        logger.warning('failed to parse pagination, err={}'.format(err))
        return []
