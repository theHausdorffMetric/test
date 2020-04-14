import datetime as dt


def get_datenow_with_offset(**offset):
    """Get local time at Dampier port, with optional offset.

    TODO could be made generic

    Args:
        offset: keyword arguments for `dt.timedelta`

    Returns:
        str: ISO-8601 formatted datetime string

    """
    return (
        dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        + dt.timedelta(**offset)
    ).strftime('%d/%m/%Y')
