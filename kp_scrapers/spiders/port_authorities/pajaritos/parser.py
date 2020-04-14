from kp_scrapers.lib.date import to_isoformat


SPANISH_TO_ENGLISH_MONTH = {
    'enero': 'january',
    'febrero': 'february',
    'marzo': 'march',
    'abril': 'april',
    'mayo': 'may',
    'junio': 'june',
    'julio': 'july',
    'agosto': 'august',
    'septiembre': 'september',
    'setiembre': 'september',
    'octubre': 'october',
    'noviembre': 'november',
    'diciembre': 'december',
}


def get_report_link(response, tab_name):
    """Get link to port activity URL within home page.

    The link to the port activity report updates about once every seven days (as of 29 March 2018).

    Args:
        response (scrapy.Response):

    Returns:
        str: url to dynamic port activity page

    """
    # get relative path from home page
    relative_path = response.xpath(
        '//div[@class="embed-container native-embed-container"]/embed/@src'
    ).extract_first()

    tokens = relative_path.split('/')
    tokens[3] = (
        tokens[3]
        .replace('_', '%20', 3)
        .replace('_', '%20%20', 2)
        .replace('_', '%20', 1)
        .replace('_', '%20%20', 1)
        .replace('_', '%20')
        .replace('.htm', '')
    )

    relative_path = '/'.join(tokens)

    # combine to form absolute url
    return 'https://www.puertocoatzacoalcos.com.mx{}_archivos/sheet{}.htm'.format(
        relative_path, tab_name
    )


def extract_reported_date(response):
    """Extract reported date from home page.

    Reported date resides just below the dynamic link and is formatted as such:
    "Última actualización: 27 de Marzo 2018"

    Args:
        response (scrapy.Response):

    Returns:
        str: reported date in ISO-8601 format

    """
    # get raw date string
    raw_str = (
        response.xpath('//div[@itemprop="articleBody"]/div/time/text()')
        .extract_first()
        .replace('Última actualización:', '')
        .lower()
        .replace('de', '')
    )

    # translate spanish months to english
    day, month, year = raw_str.split()
    return to_isoformat(' '.join([day, SPANISH_TO_ENGLISH_MONTH[month], year]))


def extract_table_and_headers(response):
    """Extract table rows and headers from port activity page.

    The first header row does not give the names for some of the sub-columns, hence we need to
    insert some elements of the second header row (which contains the name for the sub-columns)
    into the first row.

    First header row:
    ['NOMBRE DEL BUQUE', 'BANDERA', 'T.R.B.', 'ESLORA', 'CALADOS', 'E T A', 'ORIGEN', 'AGENTE\n  NAVIERO', 'TONELADAS', 'PRODUCTO', 'MUELLE']  # noqa

    Second header row:
    ['MTS.', 'PIES', 'FECHA', 'HORA', 'CARGA', 'DESCARGA', 'ASIGNADO']

    How to insert second header row to first:
    [..., 'CALADOS', 'E T A', 'ORIGEN', 'AGENTE\n  NAVIERO', 'TONELADAS', ...]
                       ^^^                                      ^^^
                     replace                                  replace
                     with                                     with
                     'FECHA',                                 'CARGA',
                     'HORA'                                   'DESCARGA'

    Args:
        response (scrapy.Response):

    Returns:
        List[scrapy.Selector], List[str]: row selectors, column headers

    """
    headers = response.xpath('//table/tr[position()=1]/td/text()').extract()
    # replace 'E T A' with 'FECHA', 'HORA'
    headers[5] = 'FECHA'
    headers.insert(6, 'HORA')

    # replace 'TONELADAS' with 'CARGA', 'DESCARGA'
    headers[9] = 'CARGA'
    headers.insert(10, 'DESCARGA')

    return response.xpath('//table/tr[position()>2]'), headers
