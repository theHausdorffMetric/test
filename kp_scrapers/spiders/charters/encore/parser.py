import re

from kp_scrapers.lib.parser import may_strip


END_OF_TABLE = 'BUNKER PRICE'
RELEVANT_FIELD = 'FIXTURES'
IRRELEVANT_FIELD = ['CARGOES', 'TENDERS']

MISSING_ROWS = []


def get_fixtures_data_table(body):
    """Parse data row (fixtures) from the raw body in format of html.

    The data format:
    ```
    //CARGOES//

    VITOL   130 NHC MEG/EAST    20-22/8     - CVRD NO DTLS

    SOIL    130 NHC YANBU/ONSAN

    SCI 95  NHC RASTAURA/MUMBAI 24/8

    //FIXTURES//

    BP  80  FO  AIN SUKHNA/MED – SPORE 23-25/8  - SCF PRIMORYE  o/p

    BP  130 NHC ZIRKU+RT/DURBAN        23/8     - LEONID LOZA   w51
    ```

    Fixtures fields are what we're interested, we store the state of whether to keep the record by
    detecting if it's relevant table field.


    Args:
        body (Selector):

    Returns:
        List[str]:

    """
    keep = False
    for raw_row in body.xpath('//text()').extract():
        row = may_strip(raw_row)

        # reach end of table, stop processing
        if END_OF_TABLE in row:
            return

        # empty row, discard
        if not row:
            continue

        # relevant data (fixtures), keep them from next row
        if RELEVANT_FIELD in row:
            keep = True
            continue

        # irrelevant data, discard them
        if any(True for x in IRRELEVANT_FIELD if x in row):
            keep = False
            continue

        # keep relevant data
        if keep:
            yield row


def restore_cells_for_row(table):
    """Restore cells of each row with partial row issue handled.

    Args:
        table List[str]:

    Yields:
        List[str]:

    """
    prev_row = None
    for joint_row in table:
        row = _try_match(joint_row)

        # a complete row
        if row:
            yield row

        # partial row
        else:
            # try combine previous row
            if prev_row:
                row = _try_match(prev_row + joint_row)

                if row:
                    prev_row = None
                    yield row

            # the beginning of partial row
            else:
                prev_row = joint_row + ' '
                continue

        if not row:
            MISSING_ROWS.append(joint_row)


def _try_match(row):
    """Link: https://regex101.com/r/t8zwdK/5/"""
    pattern = (
        r'([A-Z]{2,})\s'
        r'([0-9A-Z]+)\s'
        r'([A-Z]{2,})\s'
        r'(\D+)\s'
        r'(\d{1,2}/\d{1,2}|\d{1,2}[-–]\d{1,2}/\d{1,2}|END/\d{1,2}|MID/\d{1,2}|EAY/\d{1,2})\s'
        r'[-–]{1}\s([A-Z]*\s?[A-Z]{3,}?)\s'
        r'(.*)$'
    )

    match = re.match(pattern, row)
    return list(match.groups()) if match else None
