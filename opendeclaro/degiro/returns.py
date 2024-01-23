import polars as pl
from polars import DataFrame


class Returns:
    def __init__(self, ds: DataFrame):
        self.data = ds

    def return_on_stock(self, isin: str):
        df = self.data.filter(pl.col("isin") == isin)
        for row in df.iter_rows(named=True):
            print(row)
