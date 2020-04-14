import logging


logger = logging.getLogger(__name__)

SECTION_SIGNS = ['BUQUES EN OPERACIÓN', 'BUQUES PROGRAMADOS', 'BUQUES FONDEADOS']
END_PROCESSING = 'BUQUE'
VESSEL_NAME_IDX = 1

REPORT_DATE_PATTERN = '.*EL D\xc3\x8dA (\d{2} DE (.*))'
SPANISH_TO_MONTH = {
    'enero': 1,
    'febrero': 2,
    'marzo': 3,
    'abril': 4,
    'mayo': 5,
    'junio': 6,
    'julio': 7,
    'agosto': 8,
    'septiembre': 9,
    'setiembre': 9,
    'octubre': 10,
    'noviembre': 11,
    'diciembre': 12,
}


def parse_table(table):
    """Parse table.

    Args:
        table (List[List[str]]):

    Yields:
        Dict[str, str]:

    """
    for row in table:
        if not row:
            continue

        if any(sub in row for sub in SECTION_SIGNS):
            current_section = row
            # to handle merged column headers
            current_section.insert(12, '')
            current_section.insert(13, 'NO PELIGROSA')
            continue

        if END_PROCESSING in row:
            break

        if row[VESSEL_NAME_IDX]:
            # to handle split columns and hence missing header
            zipped_item = list(zip(current_section, row))
            raw_item = {_item[0]: _item[1] for _item in zipped_item}
            yield raw_item
