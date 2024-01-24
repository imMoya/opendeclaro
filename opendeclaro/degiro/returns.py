import polars as pl
from polars import DataFrame


# fmt: off
class Returns:
    def __init__(self, ds: DataFrame):
        self.data = ds

    def return_on_stock(self, isin: str):
        df = self.data.filter(pl.col("isin") == isin)
        for row in df.iter_rows(named=True):
            stocks_before = self.get_stocks_purchased_before(row, df)
            print(stocks_before)

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
