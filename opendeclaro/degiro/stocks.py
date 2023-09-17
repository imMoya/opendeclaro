from typing import Optional

import polars as pl
from degiro.prepare import Dataset
from polars import DataFrame


class Stocks:
    def __init__(
        self,
        path: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        self.path = path
        self.data = Dataset(path).data
        self.start_date = start_date
        self.end_date = end_date

    def return_on_stock(self, stock: str) -> DataFrame:
        df_summary = pl.DataFrame(schema=self.data.columns)
        df = self.data.filter(pl.col(["product"]) == stock)
        if self.start_date is not None:
            df = df.filter(pl.col("value_date") >= self.start_date)
        if self.end_date is not None:
            df = df.filter(pl.col("value_data") <= self.end_date)
        df.with_columns(
            (pl.col("number") * pl.col("price")).alias("amount"),
            (pl.lit(0)).alias("shares_sol"),
            (pl.lit(False).alias("conflict_2m")),
        )
