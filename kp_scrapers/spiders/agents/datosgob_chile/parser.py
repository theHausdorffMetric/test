import datetime

from lxml import html
import requests
from requests.compat import urljoin


NUMBERS_TO_MONTHS = {
    '1': 'enero',
    '2': 'febrero',
    '3': 'marzo',
    '4': 'abril',
    '5': 'mayo',
    '6': 'junio',
    '7': 'julio',
    '8': 'agosto',
    '9': 'septiembre',
    '10': 'octubre',
    '11': 'noviembre',
    '12': 'diciembre',
}


def get_months_in_year(current_date, number_of_past_dates, number_of_days):
    """ Create a dict whose keys/items are years/months of dates of interest.
    Dates of interest are chosen as the current date and past dates depending
    on given numbers of past dates and days
    Args:
        current_date (datetime.datetime)
        number_of_past_dates (int)
        number_of_days (int)
    Returns:
        Dict[str, str]: dictionary whose keys/items are years/months of interest.
    Examples:
        >>> get_months_in_year(datetime.datetime(1990, 5, 23, 0,0,0), 2, 30)
        {'1990': ['mayo', 'abril', 'marzo']}
        >>> get_months_in_year(datetime.datetime(1990, 1, 23,0,0,0), 1, 30)
        {'1990': ['enero'], '1989': ['diciembre']}

    """

    delta = datetime.timedelta(number_of_days)
    dates = [current_date]

    for n in range(number_of_past_dates):
        dates.append(current_date - (n + 1) * delta)

    for date in dates:
        year = str(date.year)
        month = NUMBERS_TO_MONTHS[str(date.month)]
        if date == dates[0]:
            months_in_year = {year: [month]}
        else:
            if year in months_in_year.keys():
                months_in_year.setdefault(year, [])
                months_in_year[year].append(month)
            else:
                months_in_year[year] = [month]

    return months_in_year


def get_file_reported_date(year, month):
    """ Provide a reported date for given (year, month) in the dict
    months_in_year.
    :param year: str
    :param month: str
    :return: datetime.datetime | None
    examples:
        >>> get_file_reported_date('2020', 'marzo')
        datetime.datetime(2020, 3, 1, 0, 0)
        >>> get_file_reported_date('2020', 'mars')
        >>> get_file_reported_date('2019', 'diciembre')
        datetime.datetime(2019, 12, 1, 0, 0)
    """
    if month in NUMBERS_TO_MONTHS.values():
        month = [key for key, value in NUMBERS_TO_MONTHS.items() if value == month][0]
        return datetime.datetime(int(year), int(month), 1)
    else:
        return None


def get_import_page(start_url, year):
    """ Query start page to filter datasets by import datasets and year.
    Then, return page of import datasets related to the queried year.
    Args :
        start_url : str
        year : str
    Returns:
        request
    """
    url_query = start_url + '?q=importacion+' + year + '&sort=score+desc%2C+metadata_modified+desc'
    query = requests.get(url_query)
    query_text = html.fromstring(query.content)
    url_import = query_text.xpath('//a[contains(@href, "importacion-%s")]/@href' % year)[0]
    return requests.get(urljoin(query.url, url_import))


def get_rar_urls(import_page, month):
    """ Find rar urls related the selected month
    Args :
        import_page : request
        month : str
    Return :
        list of urls : list[str]
    """
    import_content = html.fromstring(import_page.content)
    return import_content.xpath('//a[contains(@href, "%s")]/@href' % month)
