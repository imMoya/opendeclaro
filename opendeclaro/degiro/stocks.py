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
        df = self.data.filter(pl.col(["product"]) == stock)
        if self.start_date is not None:
            df = df.filter(pl.col("value_date") >= self.start_date)
        if self.end_date is not None:
            df = df.filter(pl.col("value_data") <= self.end_date)
        df = df.with_columns(
            (pl.col("number") * pl.col("price")).alias("amount"),
            (pl.lit(False).alias("conflict_2m")),
        ).sort("value_date")
        for row in df.filter(pl.col("action") == "sell").select("id_order").iter_rows():
            sale_df = df.filter(pl.col("id_order") == row[0])
            shares_sold = df.filter(pl.col("id_order") == row[0]).select(pl.sum("number"))
            buy_orders = df.filter(pl.col("action") == "buy").select("id_order").to_series()
            auxbuy_df = (
                df.filter(pl.col("id_order").is_in(buy_orders))
                .with_columns(pl.cumsum("number").sub(shares_sold).sub(pl.col("number")).alias("pending"))
                .filter(pl.col("pending") <= 0)
                .with_columns(
                    pl.when(abs(pl.col("pending")) > pl.col("number"))
                    .then(pl.col("number"))
                    .otherwise(abs(pl.col("pending")))
                    .alias("shares_effective")
                )
            )
            buy_df = df.filter(pl.col("id_order").is_in(auxbuy_df.select("id_order").to_series()))
            buy_df = buy_df.join(auxbuy_df.select(["id_order", "shares_effective"]), on="id_order", how="left")
            pl.concat([sale_df, buy_df], how="diagonal")
