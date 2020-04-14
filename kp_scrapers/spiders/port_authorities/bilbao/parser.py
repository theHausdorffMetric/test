from kp_scrapers.lib.parser import may_strip


REPORTED_DATE_OPTIONS = {
    # page to check for reported_date
    '-p': ['1'],
    # stream-mode extraction (more reliable if no ruling lines between cells)
    '-t': [],
    # fixed dimensions where reported date will be in
    '-a': ['35.259,181.03,88.936,803.058'],
}

TABLE_OPTIONS = {
    '-p': ['all'],
    # stream-mode extraction (more reliable if no ruling lines between cells)
    '-t': [],
    # fixed dimensions of table to extract from
    '-a': ['23.155,11.578,585.19,820.95'],  # fixed dimensions of table to extract from
    # table column coordinates
    '-c': ['48.9,135.02,154.43,182.91,270.39,357.93,445.71,' '537.36,627.85,678.71,727.23,769.77'],
}


def _preprocess_table(table):
    """Preprocess raw tabular rows extracted from the report and discard irrelevant rows

    Args:
        table (List[str]):

    Returns:
        List[str]:

    """
    is_relevant_row = False
    _table = []
    for row in table:
        # start of relevant table section
        if ''.join(row) == 'Scheduled calls':
            is_relevant_row = True
            continue

        # end of relevant table section
        if ''.join(row).startswith('C.N.:'):
            is_relevant_row = False
            continue

        if not is_relevant_row:
            continue

        _table.append(row)

    return _combine_rows(_table)


def _combine_rows(table):
    """Combine port activity data that has been split across multiple rows.

    Because of the limited table width, sometimes cell data may overflow to
    the next rows:

        # example 1
        - ['2969', 'STOLT SANDERLING', '0', 1000 productos', 'P.exterio', '13-12']
        - ['', '', '', 'agropecuarios', '', '']

        # example 2
        - ['2941', 'MN PELICAN', '2500 para plataf.,camion', '2500 para plataf.,camion', '13-12']
        - ['', '', 'carga', 'carga', '']

        # example 3
        - ['2922', 'FINNSKY', '5000 papel', '0', 'Ampliacio', 'Toro', 'Toro', '13-12']
        - ['', '', '5 teus', '0', 'Ampliacio', 'Toro', 'Toro', '13-12']
        - ['', '', '0', '3 teus', 'Ampliacio', 'Toro', 'Toro', '13-12']
        - ['', '', '0', '3500 general', 'Ampliacio', 'Toro', 'Toro', '13-12']

    The goal is to combine them to form a single row so that we can map them to a raw dict.

    Args:
        table(List[List[str]]): extracted pdf data

    Yields:
        List[str]:

    """
    for idx, row in enumerate(table):
        # row has full data
        if row[0]:
            # memoise full-row idx so we know which row to append data to
            full_row_idx = idx

        # row has partial data
        else:
            table[full_row_idx] = [f'{x} {y}' for x, y in zip(table[full_row_idx], row)]

    for row in table:
        if row[0]:
            yield [may_strip(cell) for cell in row]
