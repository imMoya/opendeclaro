from datetime import datetime, timedelta
from typing import Optional

import polars as pl
from polars import DataFrame

from .utils import opposite_transaction


class FIFO:
    def __init__(self, row: dict, df: DataFrame, opp_df: Optional[DataFrame] = None):
        self.row = row
        self.df = df
        self.opp_df = opp_df
        self.first_opp_df()

    def first_opp_df(self):
        if self.opp_df == None:
            self.opp_df = (
                self.df.sort("value_date")
                .filter(
                    (pl.col("action") == opposite_transaction(self.row["action"]))
                    & (pl.col("value_date") < self.row["value_date"])
                )
                .with_columns(
                    pl.cumsum("number").sub(self.row["shares_effective"]).sub(pl.col("number")).alias("pending")
                )
                .filter(pl.col("pending") <= 0)
                .with_columns(
                    pl.when(abs(pl.col("pending")) > pl.col("number"))
                    .then(pl.col("number"))
                    .otherwise(abs(pl.col("pending")))
                    .alias("shares_effective")
                )
            )
        else:
            pass


# fmt: off
class Returns:
    def __init__(self, ds: DataFrame, end_date: Optional[str] = None, start_date: Optional[str] = None):
        self.data = ds
        self.end_date = end_date
        self.start_date  = start_date

    def return_on_stock(self, isin: str):
        df = self.data.filter(pl.col("isin") == isin)
        opp_df = None
        for row in df.sort(pl.col("value_date")).iter_rows(named=True):
            stocks_before = self.get_stocks_purchased_before(row, df)
            if self.choose_compute_transaction(row, stocks_before) == True:
                row["date_2m_limit"] = row["value_date"] + timedelta(days=60)
                row["shares_effective"] = min(abs(stocks_before), row["number"])
                opp_df = FIFO(row, df, opp_df)


    @staticmethod
    def get_stocks_purchased_before(row: dict, df: DataFrame) -> float:
        """Returns the total number of stocks purchased for a df
        Positive: net long position
        Negative: net short position

        Parameters
        ----------
        row: dict
            dictionary containing the row of a transaction of the stocks dataframe
        df : DataFrame
            dataframe of transactions (before a particular time)

        Returns
        -------
        float
            number of stocks purchased (long if positive, short if negative, zero if no position)
        """
        stocks_purchased_before = (
            df
            .filter(
                (pl.col("date") < row["date"]) &
                (pl.col("action") == "buy")
            )
            .select(pl.col("number")).sum().item()
        )
        stocks_sold_before = (
            df
            .filter(
                (pl.col("date") < row["date"]) &
                (pl.col("action") == "sell")
            )
            .select(pl.col("number")).sum().item()
        )
        return stocks_purchased_before - stocks_sold_before
    
    @staticmethod
    def choose_compute_transaction(row: dict, stocks_before: float) -> bool:
        if (row["action"] == "sell") & (stocks_before <= 0):
            return False
        if (row["action"] == "sell") & (stocks_before > 0):
            return True
        if (row["action"] == "buy") & (stocks_before >= 0):
            return False
        if (row["action"] == "buy") & (stocks_before < 0):
            return True


# fmt: on
