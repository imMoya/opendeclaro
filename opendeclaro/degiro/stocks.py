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
        # fmt: off
        
        df = self.data.filter(pl.col(["product"]) == stock)
        
        # filter start_date and end_date
        if self.start_date is not None:
            df = df.filter(pl.col("value_date") >= self.start_date)
        if self.end_date is not None:
            df = df.filter(pl.col("value_data") <= self.end_date)
        
        # create additional columns to df: "amount" "conflict_2m"
        df = (
            df
            .with_columns(
                (pl.col("number") * pl.col("price")).alias("amount"),
                (pl.lit(False).alias("conflict_2m")),
            ).sort("value_date")
        )

        # start iterating through sales of stock
        for row in df.filter(pl.col("action") == "sell").select("id_order").iter_rows():
            # compute sell 
            sale_df = df.filter(pl.col("id_order") == row[0])
            auxsell_df = (
                df
                .filter(
                    (pl.col("id_order") == row[0]) & 
                    (pl.col("action") == "sell")
                    )
                .with_columns(pl.col("number").alias("shares_effective"))
            )
            sale_df = (
                sale_df
                .select(pl.all().exclude("number"))
                .join(auxsell_df.select(["id_order", "number", "shares_effective"]), on="id_order", how="outer")
            )
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
            buy_df = (
                buy_df
                .select(pl.all().exclude("number"))
                .join(auxbuy_df.select(["id_order", "shares_effective", "number"]), on="id_order", how="inner")
            )
            all_df = pl.concat([sale_df, buy_df], how="diagonal")
            return_sale = all_df.select((pl.col("var") * pl.col("shares_effective") / pl.col("number"))).sum()
        # fmt: on
