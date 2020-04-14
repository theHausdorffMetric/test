import logging

from kp_scrapers.lib.parser import may_strip


TABULA_OPTIONS_FIND = {
    '-p': ['all'],  # pages to extract
    '-g': [],  # guess dimensions of table to extract data from
    '-l': [],  # lattice-mode extraction (more reliable with table borders)
}
logger = logging.getLogger(__name__)


def _parse_pdf(table_rows):
    start_processing = False
    berth = ''
    for row in table_rows:
        row = [may_strip(cell) for cell in row]
        berth_num = may_strip(row[0].split('(')[0])
        if berth_num.isdigit():
            berth = berth_num
        check = ['IMO', 'DWT']
        if set(check).issubset(set(row)):
            headers = row
            # column corresponding to movement is not well parsed need to adjust manually
            index_movement = headers.index('Produto Cargo') - 1
            if headers[index_movement] == '':
                headers[index_movement] = 'movement'
            else:
                logger.info(f'check parsing pdf columns')
                return None
            start_processing = True
            continue
        if not start_processing or len(row) != 18 or (row[2] == '' and row[3] == ''):
            continue
        # last line special case parsing:
        if row[0] != '' and row[len(row) - 1] == '':
            row.insert(0, '')
            row.pop()
        row[0] = berth
        raw_item = {head: row[idx] for idx, head in enumerate(headers)}
        yield raw_item
