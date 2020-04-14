"""
Excel file has multiple tabs, each tab can have different header index
Dictionary contains the relevant row to start, vessel column to ignore
irrelevant filler row, followed by the header list for each tab

Dict:
    tuple(
        Indicate valid row(int),
        Indicate vessel col(int),
        header(list),
    )

"""
HEADER_SHEET_MAPPING = {
    'port': (
        4,
        1,
        [
            'berth',
            'vessel',
            'nationality',
            'agent',
            'arrival_date',
            'arrival_time',
            'loa',
            'gt',
            'fwd',
            'aft',
            'berthed_date',
            'berthed_time',
            'cargo',
            'total',
            'remarks',
        ],
    ),
    'anchorage': (
        3,
        2,
        [
            'number',
            'arrival_date',
            'arrival_time',
            'vessel',
            'nationality',
            'agent',
            'loa',
            'gt',
            'fwd',
            'aft',
            'terminal',
            'cargo',
            'tonnage',
            'purpose',
        ],
    ),
    'expected': (
        3,
        2,
        [
            'number',
            'eta_date',
            'vessel',
            'nationality',
            'agent',
            'loa',
            'gt',
            'fwd',
            'aft',
            'cargo',
            'terminal',
            'tonnage',
        ],
    ),
    'permanent vesls': (
        3,
        2,
        [
            'number',
            'arrival_date',
            'arrival_time',
            'vessel',
            'nationality',
            'agent',
            'gt',
            'loa',
            'fwd',
            'aft',
            'terminal',
            'cargo',
            'tonnage',
            'purpose',
        ],
    ),
    'wrecked': (
        3,
        3,
        [
            'number',
            'arrival_date',
            'arrival_time',
            'vessel',
            'nationality',
            'agent',
            'loa',
            'gt',
            'fwd',
            'aft',
            'terminal',
            'cargo',
            'tonnage',
            'purpose',
            'position',
        ],
    ),
    'beached vessel': (
        3,
        3,
        [
            'number',
            'arrival_date',
            'arrival_time',
            'vessel',
            'nationality',
            'agent',
            'loa',
            'gt',
            'fwd',
            'aft',
            'terminal',
            'cargo',
            'tonnage',
            'purpose',
            'position',
        ],
    ),
}
