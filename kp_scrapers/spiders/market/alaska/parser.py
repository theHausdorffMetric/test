import datetime as dt

from dateutil.relativedelta import relativedelta


# month-first format, because america
SRC_DATE_FMT = '%m/%d/%Y'


def get_first_day_of_current_month():
    """Get the first date of the current month.

    AlaskaInventories source displays data with a psuedo "month-to-date" query, where it will
    only display data after the queried date.

    Therefore to get all data for the current month, we need to use the first day of the month.

    """
    return dt.datetime.utcnow().replace(day=1).strftime(SRC_DATE_FMT)


def get_date_range(start_date, end_date):
    """Get all months inclusive between starting date and ending date.

    Examples:
        >>> list(get_date_range('06/23/2019', '08/09/2019'))
        ['06/01/2019', '07/01/2019', '08/01/2019']

    """
    start_date = dt.datetime.strptime(start_date, SRC_DATE_FMT).replace(day=1)
    end_date = dt.datetime.strptime(end_date, SRC_DATE_FMT).replace(day=1)

    # find the difference in month betwwen start and end_date
    time_gap = diff_month(end_date, start_date)

    for date in range(time_gap + 1):
        day = start_date + relativedelta(months=date)
        yield day.strftime(SRC_DATE_FMT)


def parse_input_date(date, **offset):
    """Given an AlaskanInventories date, convert it to an ISO-8601 format (with optional offset).

    Examples:
        >>> parse_input_date('08/09/2009')
        '2009-08-09T00:00:00'
        >>> parse_input_date('08/09/2009', days=1)
        '2009-08-10T00:00:00'

    """
    return (dt.datetime.strptime(date, SRC_DATE_FMT) + dt.timedelta(**offset)).isoformat()


def diff_month(d1, d2):
    """Get the number of months separating two dates.

    Examples:
        >>> diff_month(dt.datetime(2019,8,9,0,0,0),dt.datetime(2019,3,9,0,0,0))
        5
        >>> diff_month(dt.datetime(2019,8,9,0,0,0),dt.datetime(2018,3,9,0,0,0))
        17

    """
    return (d1.year - d2.year) * 12 + d1.month - d2.month
