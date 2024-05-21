from datetime import datetime
from typing import Optional

import polars as pl
from polars import DataFrame


def opposite_transaction(action: str):
    opposite = {"buy": "sell", "sell": "buy"}
    assert action in opposite, "Invalid action. Please provide 'buy' or 'sell'."
    return opposite[action]


def datestr_to_datetime(datestr: str):
    date_format = "%d/%m/%Y"
    return datetime.strptime(datestr, date_format)


# fmt: off
def filter_df_inside_dates(
    df: DataFrame, col_name: str = "value_date", start_date: Optional[str] = None, end_date: Optional[str] = None
) -> DataFrame:
    if (start_date is None) and (end_date is None):
        return df
    elif (start_date is not None) and (end_date is None):
        return df.filter(pl.col(col_name)>datestr_to_datetime(start_date))
    elif (start_date is None) and (end_date is not None):
        return df.filter(pl.col(col_name)<datestr_to_datetime(end_date))
    elif (start_date is not None) and (end_date is not None):
        return (
            df.filter(
                (pl.col(col_name)>datestr_to_datetime(start_date)) &
                (pl.col(col_name)<datestr_to_datetime(end_date))
            )
        )
# fmt: on


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
