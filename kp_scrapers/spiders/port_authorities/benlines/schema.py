
from enum import Enum, unique, auto


@unique
class BenlineTableEnum(Enum):
    LOADING = auto()
    DISCHARGE = auto()
    WAITING = auto()
    ARRIVE = auto()


# tables expected to be found in the pdf document
# this is a map key,value with :
#   key : id of the table
#   value : array of column names
table_columns = {
    BenlineTableEnum.LOADING:
        ['Vessel Name Berth Name Activity / Cargo / Quantity Arrival Date Berth Date', '', 'ETC', 'Remarks'],
    BenlineTableEnum.DISCHARGE:
        ['Vessel Name Berth Name Activity / Cargo / Quantity Arrival Date Berth Date', '', 'ETC', 'Remarks'],
    BenlineTableEnum.WAITING:
        ['Vessel Name Activity / Cargo / Quantity Arrival Date', '', 'Remarks', ''],
    BenlineTableEnum.ARRIVE:
        ['Vessel Name Activity / Cargo / Quantity ETA', '', 'Remarks', ''],
}

table_labels = {
    BenlineTableEnum.LOADING: 'VESSELS AT BERTH FOR  LOADING',
    BenlineTableEnum.DISCHARGE: 'VESSELS AT BERTH FOR  DISCHARGE',
    BenlineTableEnum.WAITING: 'VESSELS WAITING FOR BERTH',
    BenlineTableEnum.ARRIVE: 'VESSELS EXPECTED TO ARRIVE PORT',
}


def columns_of_table(id):
    return table_columns.get(id)


def label_of_table(id):
    return table_labels.get(id)


def table_id_of_label(label):
    if label == 'VESSELS AT BERTH FOR  LOADING':
        return BenlineTableEnum.LOADING

    if label == 'VESSELS AT BERTH FOR  DISCHARGE':
        return BenlineTableEnum.DISCHARGE

    if label == 'VESSELS WAITING FOR BERTH':
        return BenlineTableEnum.WAITING

    if label == 'VESSELS EXPECTED TO ARRIVE PORT':
        return BenlineTableEnum.ARRIVE

    return None
