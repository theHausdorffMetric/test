import logging
import re

from dateutil.parser import parse as parse_date

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import map_keys
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.units import Unit
from kp_scrapers.models.utils import validate_item


logger = logging.getLogger(__name__)


@validate_item(SpotCharter, normalize=True, strict=False)
def process_item(raw_item):
    """Transform raw item to relative model.

    Args:
        raw_item (Dict[str, str]):

    Returns:
        Dict[str | Dict[str]]:

    """
    item = map_keys(raw_item, field_mapping(), skip_missing=True)

    # build vessel sub model
    vessel_name, volume = normalize_vessel_name(item.pop('vessel_volume', ''))
    if not vessel_name:
        return
    item['vessel'] = {'name': vessel_name}

    # build cargo sub model
    item['cargo'] = {
        'product': item.pop('product', ''),
        'volume': item.pop('volume', '') or volume,
        'volume_unit': Unit.kilotons,
        'movement': 'load',
    }

    item['lay_can_start'], item['lay_can_end'] = normalize_lay_can(
        item.pop('lay_can', ''), item['reported_date']
    )

    return item


def field_mapping():
    return {
        '0': ('vessel_volume', None),
        '1': ('volume', None),
        '2': ('product', None),
        '3': ('departure_zone', None),
        '4': ('lay_can', None),
        '5': ('arrival_zone', normalize_arrival_zone),
        '6': ('charterer', None),
        'provider_name': ('provider_name', None),
        'reported_date': ('reported_date', None),
    }


def normalize_vessel_name(raw_vessel_name):
    """Normalize vessel name.

    Examples:
        >>> normalize_vessel_name('Hafnia Henriette 37')
        ('Hafnia Henriette', '37')
        >>> normalize_vessel_name('TBN')
        (None, None)

    Args:
        raw_vessel_name (str):

    Returns:
        Dict[str, str]:

    """
    if 'TBN' in raw_vessel_name.split():
        return None, None

    _last_word = raw_vessel_name.split()[-1]
    if re.match(r'\d+', _last_word):
        return raw_vessel_name.replace(_last_word, '').strip(), _last_word

    return raw_vessel_name, None


def normalize_arrival_zone(arrival_zone):
    """Normalize arrival zone.

    Args:
        arrival_zone (str):

    Returns:
        List[str]:

    """
    if arrival_zone.upper().strip() == 'UNKNOWN':
        return None

    return [alias.strip() for alias in arrival_zone.split('/')]


def normalize_lay_can(raw_lay_can, reported):
    """Normalize lay can date with reported year as reference.

    Lay can date pattern:
        1. cross month case: Aug 29-Sep 1
        2. duration case: Sep 1-2
        3. single day case: Oct. 23

    Examples:
        >>> normalize_lay_can('Aug 29-Sep 1', '25 Oct 2018')
        ('2018-08-29T00:00:00', '2018-09-01T00:00:00')
        >>> normalize_lay_can('Sep 1-2', '25 Oct 2018')
        ('2018-09-01T00:00:00', '2018-09-02T00:00:00')
        >>> normalize_lay_can('Oct. 23', '25 Oct 2018')
        ('2018-10-23T00:00:00', '2018-10-23T00:00:00')
        >>> normalize_lay_can('Dec 28-Jan 1', '1 Jan 2019')
        ('2018-12-28T00:00:00', '2019-01-01T00:00:00')

    Args:
        raw_lay_can (str):
        reported (str):

    Returns:
        Tuple[str, str]:

    """
    _finall = re.findall(r'([A-Za-z]{3,4})\D+(\d{1,2})\D?(\d{1,2})?', raw_lay_can)

    if not _finall:
        logger.error(f'Unable to parse this lay can date: {raw_lay_can}')
        return None, None

    # cross month case
    if len(_finall) == 2:
        lay_can_start, _ = _build_lay_can_date(_finall[0], reported)
        lay_can_end, _ = _build_lay_can_date(_finall[1], reported)
        return lay_can_start, lay_can_end

    if len(_finall) == 1:
        lay_can_start, lay_can_end = _build_lay_can_date(_finall[0], reported)
        lay_can_end = lay_can_end if lay_can_end else lay_can_start
        return lay_can_start, lay_can_end


def _build_lay_can_date(date_tuple, reported):
    """Build lay can date given date tuple.

    As there's no year, we refer to reported year. Specific scenario:
        -------------------------------------------------------
        | Lay can month   |   Reported date  |   Lay can year |
        | Dec             |   1 Jan 2019     |   2018         |
        | Jan             |   28 Dec 2018    |   2019         |
        | Dec - Jan       |   1 Jan 2019     |   2018 / 2019  |
        -------------------------------------------------------

    Args:
        date_tuple (Tuple[str, str, str]): (month, start day, end day), end day might be empty
        reported (str): reported date

    Returns:
        Tuple[str, str]: first date, second date

    """
    year = parse_date(reported).year
    month, first_day, second_day = date_tuple
    if month == 'Dec' and 'Jan' in reported:
        year -= 1
    if month == 'Jan' and 'Dec' in reported:
        year += 1

    first_date = to_isoformat(f'{first_day} {month} {year}', dayfirst=True)
    second_date = (
        to_isoformat(f'{second_day} {month} {year}', dayfirst=True) if second_day else None
    )

    return first_date, second_date
