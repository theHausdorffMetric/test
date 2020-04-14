import logging

from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta
import requests

from kp_scrapers.lib.services.shub import global_settings as Settings
from kp_scrapers.lib.utils import map_row_to_dict


logger = logging.getLogger(__name__)

# all 3 platforms share the same credentials
KP_API_BASE = Settings()['KP_API_BASE']
KP_API_CREDENTIALS = {
    'email': Settings()['KP_API_EMAIL'],
    'password': Settings()['KP_API_PASSWORD'],
}

# date format to use when POSTing requests
KP_API_DATE_PARAM_FORMAT = '%Y-%m-%d'

# store session state so we don't have to re-authenticate for each call
_SESSION = None


class KplerApiService(object):
    """Interface with Kpler API.

    Access data from Kpler API to supplement data sources lacking certain fields to compute
    attributes like lay_can_start and lay_can_end.

    """

    def __init__(self, platform_name):
        self.platform_name = platform_name
        self.token = self._connect().get('token')

    def _connect(self):
        login_res = requests.post(
            KP_API_BASE.format(self.platform_name, 'login'), json=KP_API_CREDENTIALS
        )
        if login_res.status_code != 200:
            raise ConnectionError('Credentials for api-oil is incorrect or missing.')

        return login_res.json()

    def get_trades(self, params):
        """Get trades based on query parameters from "/trades" endpoint.

        Args:
            params (Dict[str, str]): dictionary of query parameters

        Yields:
            Dictp[str, str]: response containing resulting trade

        """
        req_str = KP_API_BASE.format(self.platform_name, 'trades')
        trade_res = requests.get(req_str, params=params, headers={'Authorization': self.token})
        trade_rows = trade_res.text.splitlines()
        row_count = len(trade_rows)
        if row_count < 2:
            logger.info(
                f'No trades found for vessel {params.get("vessels")} '
                f'at loading port {params.get("zonesorigin")} '
                f'from {params.get("startdate")} to {params.get("enddate")}'
            )
            return

        header = trade_rows[0].split(';')  # ';' is the delimiter for response text
        for idx in range(1, row_count):  # data starts from row 1
            yield map_row_to_dict(trade_rows[idx].split(';'), header)

    def get_import_trade(self, vessel, origin, dest, end_date):
        """Get full trade given trade destination data.

        First try to call the API with origin parameter (to improve accuracy).
        If no trades are returned, call the API without the origin parameter.
        Match trades by checking that both destination date and installations are
        accurate to what's stipulated in the report (return the first one that matches).
        Finally, get the lay_can_start and lay_can_end from the final matched trade.

        Args:
            vessel (str): name of vessel
            origin (str): starting zone of trade
            dest (str): end zone of trade
            end_date (str): end date of trade (MUST be in ISO8601 format)

        Returns:
            Dict[str, str] | None: trade dict if there is a successful match, else None

        """
        # sanity check that date exists and can be parsed as date
        if not end_date or not self.is_date(end_date):
            return None

        end_date = parse_date(end_date, dayfirst=False)
        # define search timeframe to be -4/+1 month from import_date (c.f. analysts)
        # dates are formatted according to Kpler API specification.
        _start_date = (end_date - relativedelta(months=4)).strftime(KP_API_DATE_PARAM_FORMAT)
        _end_date = (end_date + relativedelta(months=1)).strftime(KP_API_DATE_PARAM_FORMAT)
        # get all trades within timeframe for the vessel with origin parameter
        params = {
            'vessels': vessel.lower(),
            'startDate': _start_date,
            'endDate': _end_date,
            'toZones': dest.lower(),
            'fromZones': origin.lower(),
        }
        trades = list(self.get_trades(params))

        # if no trades matched, relax the search criteria by removing the origin_zone
        if len(trades) == 0:
            params.pop('fromZones')
            trades = list(self.get_trades(params))

        # sanity check, in case we match to an irrelevant port call
        for trade in trades:
            if self._match_trade(trade, end_date):
                return trade

        return None

        # lay_can_start is 2 days before origin date, lay_can_end is 1 day after origin date

    @staticmethod
    def _match_trade(trade, end_date):
        """Match a given trade based on the supplied end_date.

        For each trade obtained from the API, conduct the following sanity checks (c.f. analysts):
            - trade's destination date is +/- 1 week from the import date stated in the report

        If sanitised successfully, return the matched trade.

        Args:
            trade (Dict[str, str]):
            end_date (str): date MUST be in ISO8601 format

        Returns:
            Dict[str, str] | None: trade dict if sanity checks passed, else None

        """
        # some trades are still ongoing and might not have a date at destination
        if trade['Date (destination)']:
            destination_date = parse_date(trade['Date (destination)'], dayfirst=False)

            _start_date = destination_date - relativedelta(days=7)
            _end_date = destination_date + relativedelta(days=7)

            # sanity check if the import_date falls within date range
            if end_date > _start_date < _end_date:
                # sanity check if port stated in report matches that of trade's destination
                logger.info(f'Matched trade: {trade["Zone Origin"]} -> {trade["Zone Destination"]}')
                return trade

        return None

    @staticmethod
    def is_date(input_string):
        """check if string is date type
        Args:
            string (str):

        Returns:
            str: ISO-8601 formatted matching date
        """
        try:
            parse_date(input_string)
            return True

        except ValueError:
            return False


def get_session(platform, recreate=False):
    """Get Kpler API service session, if it exists.

    This function will persist the session until the spider is completed.

    Args:
        platform (str):
        recreate (bool): refresh API session

    Returns:
        KplerApiService:

    """
    global _SESSION
    if recreate or not _SESSION:
        _SESSION = KplerApiService(platform)

    return _SESSION
