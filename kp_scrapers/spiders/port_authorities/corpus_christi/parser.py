from copy import deepcopy
import logging


logger = logging.getLogger(__name__)


_SHIP_CATEGORY = {
    'barge': '104010',
    'cargo_vessel': '104030',
    'offshore_rig': '104020',
    'tug': '104040',
}

_SHIP_CARGO_TYPE = {'dry': '403020', 'liquid': '403030', 'liquid_and_dry': '403010'}

_ACTIVITY = {
    'anchorage_inbound': '6920',
    'anchored': '6930',
    'departed': '6970',
    'docked': '6940',
    'eta': '6982',
    'inbound': '6910',
    'outbound': '6960',
    'scheduled': '6980',
    'shifting': '6950',
}

_DOCK = {
    'ACC': '129',
    'AGG': '33',
    'AKR': '108',
    'BAY': '36',
    'BSY': '101',
    'BT1': '56',
    'BT2': '57',
    'BT3': '122',
    'C01': '58',
    'C02': '59',
    'C08': '60',
    'C09': '61',
    'C10': '66',
    'C12': '67',
    'C14': '68',
    'C15': '62',
    'C7E': '92',
    'C7W': '90',
    'CBD': '116',
    'CGD': '41',
    'CH1': '296',
    'CMP': '110',
    'CPE': '104',
    'CT1': '37',
    'CT2': '38',
    'CT3': '39',
    'CT6': '40',
    'CT8': '55',
    'FDC': '45',
    'FI5': '130',
    'GC': '298',
    'GMN': '239',
    'HEI': '126',
    'HEL': '46',
    'HI1': '111',
    'HI2': '112',
    'HI3': '113',
    'HTX': '49',
    'IE': '50',
    'INM': '109',
    'INO': '51',
    'K1': '98',
    'K2': '99',
    'K3': '100',
    'KIN': '97',
    'KT': '48',
    'KWF': '119',
    'KWT': '117',
    'MAR': '255',
    'MCD': '114',
    'MI1': '261',
    'MI2': '262',
    'MI3': '268',
    'MI4': '269',
    'MI5': '270',
    'MI6': '271',
    'MI7': '272',
    'MM': '297',
    'N16': '256',
    'NB': '74',
    'NSE': '127',
    'NSI': '123',
    'NSW': '128',
    'NUS': '253',
    'O01': '75',
    'O02': '76',
    'O03': '77',
    'O04': '78',
    'O05': '73',
    'O06': '79',
    'O07': '80',
    'O08': '81',
    'O09': '82',
    'O10': '84',
    'O11': '85',
    'O12': '86',
    'O14': '252',
    'O15': '254',
    'OCC': '42',
    'OO0': '89',
    'OXY': '91',
    'REN': '93',
    'RPR': '63',
    'RPU': '64',
    'SWE': '52',
    'TD1': '118',
    'TD2': '257',
    'TD3': '125',
    'TD4': '258',
    'TD5': '259',
    'TSC': '102',
    'UNS': '107',
    'V1B': '106',
    'V1C': '105',
    'V1E': '95',
    'V1W': '83',
    'V2': '96',
    'V3': '103',
    'VO1': '260',
    'WBM': '273',
}

_MOVEMENT_STATUS = {
    'active': '730',
    'billed': '737',
    'cancelled': '750',
    'closed': '740',
    'complete': '735',
    'confirmed': '733',
    'draft': '701',
    'planned': '708',
    'ready': '725',
    'rejected': '752',
    'requested': '705',
    'scheduled': '710',
    'submitted': '736',
}


class VesselMovementQuery:
    """Pretty API around POST requests for querying vessel movements."""

    _supported_options = {
        'SHIP_CATEGORY': _SHIP_CATEGORY,
        'SHIP_CARGO_TYPE': _SHIP_CARGO_TYPE,
        'ACTIVITY': _ACTIVITY,
        'DOCK': _DOCK,
        'MOVEMENT_STATUS': _MOVEMENT_STATUS,
    }
    _form_template = {'reportCode': 'USCRP-WEB-0001', 'parameters': []}

    def __init__(self, **options):
        # cache form options
        self._options = {}
        self.query(**options)

    def query(self, **options):
        """Fluent method for adding query options to POST form."""
        for k, v in options.items():
            value_map = self._map_option(k, v)
            if not value_map:
                logger.warning("Form option not supported: %s", k)
            else:
                self._options[k] = value_map

    def to_dict(self):
        # standardised interface for obtaining instance as dict
        form = deepcopy(self._form_template)
        form.update(parameters=[self._form_option(k, v) for k, v in self._options.items()])
        return form

    def _form_option(self, key, value):
        return {'sName': key, 'aoValues': [{'Value': value}]}

    def _map_option(self, key, value):
        return self._supported_options.get(key, {}).get(value)
