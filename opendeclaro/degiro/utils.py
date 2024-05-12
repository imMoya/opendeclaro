from datetime import datetime
from typing import Optional


def opposite_transaction(action: str):
    opposite = {"buy": "sell", "sell": "buy"}
    assert action in opposite, "Invalid action. Please provide 'buy' or 'sell'."
    return opposite[action]


def datestr_to_datetime(datestr: str):
    date_format = "%d/%m/%Y"
    return datetime.strptime(datestr, date_format)


def filter_rowdate_inside_dates(rowdate: datetime, start_date: Optional[str] = None, end_date: Optional[str] = None):
    if (start_date is None) & (end_date is None):
        return True
    elif start_date is None:
        if rowdate < datestr_to_datetime(end_date):
            return True
    elif end_date is None:
        if rowdate > datestr_to_datetime(start_date):
            return True
    elif (rowdate > datestr_to_datetime(start_date)) & (rowdate < datestr_to_datetime(end_date)):
        return True
    else:
        return False
