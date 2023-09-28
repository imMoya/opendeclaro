from datetime import timedelta
from typing import Optional

import polars as pl
from degiro.prepare import Dataset
from polars import DataFrame, Series


class SaleOfStock:
    def __init__(self, df: DataFrame, stock: str, id_order: str):
        self.df = df
        self.stock = stock
        self.id_order = id_order
        self._raw_sale_df = self.raw_sale_df()
        self._aux_sale_df = self.aux_sale_df()

    # fmt: off

    @property
    def date_sale(self):
        return (
            self._raw_sale_df
            .filter(pl.col("action") == "sell")
            .select(pl.col("value_date")).item()
        )

    @property
    def date_two_month_lim(self):
        return self.date_sale + timedelta(days=60)

    @property
    def sale_df(self):
        _sale_df = (
            self.df
            .with_columns(
                pl.when((pl.col("value_date") < self.date_two_month_lim) & (pl.col("value_date") > self.date_sale))
                .then(False)
                .otherwise(True)
                .alias("two_month_violation")
            )
            .filter(pl.col("id_order") == self.id_order)
        )
        return (
            _sale_df
            .select(pl.all().exclude("number"))
            .join(
                self._aux_sale_df
                .select(["id_order", "number", "shares_effective"]), on="id_order", how="outer"
            )
        )
    
    @property
    def shares_sold(self):
        return (
            self.df
            .filter(pl.col("id_order") == self.id_order)
            .select(pl.sum("number"))
            .item()
        )

    def raw_sale_df(self):
        return (
            self.df
            .filter(
                (pl.col("id_order") == self.id_order) & 
                (pl.col("product") == self.stock)
            )
        )

    def aux_sale_df(self):
        return (
            self.df
            .filter(
                (pl.col("id_order") == self.id_order) &
                (pl.col("action") == "sell")
            )
            .with_columns(
                pl.col("number")
                .alias("shares_effective")
            )
        )

    # fmt: on


class PurchaseOfStock(SaleOfStock):
    def __init__(self, df: DataFrame, stock: str, id_order: str):
        super().__init__(df, stock, id_order)
        self._aux_purchase_df = self.aux_purchase_df()
        self._raw_purchase_df = self.raw_purchase_df()

    # fmt: off

    @property
    def buy_orders(self):
        return (
            self.df.
            filter(
                (pl.col("action") == "buy") &
                (pl.col("product") == self.stock)
            )
            .select("id_order")
            .to_series()
        )
    
    @property
    def purchase_df(self) -> DataFrame:
        return (
            self._raw_purchase_df
            .select(pl.all().exclude("number"))
            .join(
                self._aux_purchase_df.select(["id_order", "shares_effective", "number"]), on="id_order", how="inner"
            )
        )

    
    def aux_purchase_df(self) -> DataFrame:
        return (
            self.df
            .filter(pl.col("id_order").is_in(self.buy_orders))
            .with_columns(pl.cumsum("number").sub(self.shares_sold).sub(pl.col("number")).alias("pending"))
            .filter(pl.col("pending") <= 0)
            .with_columns(
                pl.when(abs(pl.col("pending")) > pl.col("number"))
                .then(pl.col("number"))
                .otherwise(abs(pl.col("pending")))
                .alias("shares_effective")
            )
        )
    
    def raw_purchase_df(self) -> DataFrame:
        return(
            self.df
            .filter(
                pl.col("id_order")
                .is_in(self._aux_purchase_df.select("id_order").to_series())
            )
        )

    # fmt: on


class Stocks:
    def __init__(
        self,
        path: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        """Initialise class

        Parameters
        ----------
        path : str
            path location of dataset csv
        start_date : Optional[str], optional
            starting date to filter dataframe, by default None
        end_date : Optional[str], optional
            ending date to filter dataframe, by default None
        """
        self.path = path
        self.data = Dataset(path).data
        self.start_date = start_date
        self.end_date = end_date

    def return_on_stock(self, stock: str) -> float:
        # fmt: off
        return_global = 0

        df = self.data.filter(pl.col(["product"]) == stock)

        # filter start_date and end_date
        if self.start_date is not None:
            df = df.filter(pl.col("value_date") >= self.start_date)
        if self.end_date is not None:
            df = df.filter(pl.col("value_data") <= self.end_date)

        # start iterating through sales of stock
        for row in df.filter(pl.col("action") == "sell").select("id_order").iter_rows():
            sale_df = df.filter(pl.col("id_order") == row[0])

            # add column of possible two month limit restriction
            date_sale = sale_df.filter(pl.col("action") == "sell").select(pl.col("value_date")).item()
            date_2m_limit = date_sale + timedelta(days=60)
            sale_df = df.with_columns(
                pl.when((pl.col("value_date") < date_2m_limit) & (pl.col("value_date") > date_sale))
                .then(False)
                .otherwise(True)
                .alias("two_month_violation")
            ).filter(pl.col("id_order") == row[0])

            # compute sell
            auxsell_df = df.filter((pl.col("id_order") == row[0]) & (pl.col("action") == "sell")).with_columns(
                pl.col("number").alias("shares_effective")
            )
            sale_df = sale_df.select(pl.all().exclude("number")).join(
                auxsell_df.select(["id_order", "number", "shares_effective"]), on="id_order", how="outer"
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
            buy_df = buy_df.select(pl.all().exclude("number")).join(
                auxbuy_df.select(["id_order", "shares_effective", "number"]), on="id_order", how="inner"
            )
            all_df = pl.concat([sale_df, buy_df], how="diagonal")
            return_sale = all_df.select((pl.col("var") * pl.col("shares_effective") / pl.col("number"))).sum().item()

            if (return_sale < 0) & (sale_df.select("two_month_violation")[0].item() == True):
                pass
            else:
                return_global += return_sale

        return return_global
        # fmt: on
