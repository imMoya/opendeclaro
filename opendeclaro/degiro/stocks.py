from datetime import datetime, timedelta
from typing import Optional

import polars as pl
from degiro.dataset import Dataset
from polars import DataFrame, Series


class SaleOfStock:
    def __init__(self, ds: Dataset, stock: str, id_order: str):
        """Class to compute the sell of a stock

        Parameters
        ----------
        ds : Dataset
            Dataset class from degiro.dataset
        stock : str
            financial product involved
        id_order : str
            id order of sale
        """
        self.df = ds.data.filter(pl.col("product") == stock)
        self.stock = stock
        self.id_order = id_order
        self._raw_sale_df = self.raw_sale_df()
        self._aux_sale_df = self.aux_sale_df()

    # fmt: off

    @property
    def date_sale(self) -> datetime:
        """Date of sale of stock

        Returns
        -------
        datetime
            date of sale of stock
        """
        return (
            self._raw_sale_df
            .filter(pl.col("action") == "sell")
            .select(pl.col("value_date")).item()
        )

    @property
    def date_two_month_lim(self) -> datetime:
        """Two month date after sale of stock

        Returns
        -------
        datetime
            two month date after sale of stock
        """
        return self.date_sale + timedelta(days=60)

    @property
    def sale_df(self) -> DataFrame:
        """Sale of stock and costs associated with order id

        Returns
        -------
        DataFrame
            contains rows which correspond to sale and other rows 
            with the associated costs of the sale
        """
        two_month_val_df = len(
            self.df.filter( 
                (pl.col("action") == "buy") & 
                (pl.col("value_date") < self.date_two_month_lim) & 
                (pl.col("value_date") > self.date_sale)
            )
        )
        two_month_val = True if two_month_val_df > 0 else False
        __sale_df = (
            self.df
            .with_columns(
                pl.lit(two_month_val)
                .alias("two_month_violation")
            )
            .filter(pl.col("id_order") == self.id_order)
        )
        return (
            __sale_df
            .select(pl.all().exclude("number"))
            .join(
                self._aux_sale_df
                .select(["id_order", "number", "shares_effective"]), on="id_order", how="outer"
            )
        )
    
    @property
    def shares_sold(self) -> float:
        """Get total number of shares sold in transaction

        Returns
        -------
        float
            total number of shares sold
        """
        return (
            self.df
            .filter(pl.col("id_order") == self.id_order)
            .select(pl.sum("number"))
            .item()
        )

    def raw_sale_df(self) -> DataFrame:
        """Get DataFrame of rows involved in stock sale (including costs)

        Returns
        -------
        DataFrame
            contains only rows involved in stock sale (including costs)
        """
        return (
            self.df
            .filter(
                (pl.col("id_order") == self.id_order) & 
                (pl.col("product") == self.stock)
            )
        )

    def aux_sale_df(self) -> DataFrame:
        """Get DataFrame of rows involved in stock sale (excluding costs)

        Returns
        -------
        DataFrame
            contains only rows involved in stock sale (excluding costs)
        """
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


class PurchaseOfStockFromSale(SaleOfStock):
    def __init__(self, ds: Dataset, stock: str, id_order: str, change_isin: bool = False):
        """Class to associate stocks purchased for a given sale order

        Parameters
        ----------
        ds : Dataset
            Dataset class from degiro.dataset
        stock : str
            financial product involved
        id_order : str
            id order of sale
        change_isin: bool
            False if sale not associated to change in isin of stock
            True if sale associated to change in isin of stock
        """
        super().__init__(ds, stock, id_order)
        if change_isin is True:
            all_stocks_df = pl.DataFrame(pl.Series("product", [stock, ds.change_isin[stock]]))
            self.df = ds.data.join(all_stocks_df, on="product", how="inner")
        self.__aux_purchase_df = self.aux_purchase_df()
        self.__raw_purchase_df = self.raw_purchase_df()

    # fmt: off

    @property
    def df_older_sales(self) -> DataFrame:
        """Compute the sales that took place before the given sale id_order

        Returns
        -------
        DataFrame
            contains previous sales (those before the current sale id_order)
        """
        return (
            self.df
            .filter(
                (pl.col("action") == "sell") & (pl.col("value_date") < self.date_sale)
            )
            .sort("value_date")
        )
    
    @property
    def purchase_df_after_prev_sales(self) -> DataFrame:
        """Compute the dataframe with sales available after older sales

        Returns
        -------
        DataFrame
            contains the available shares for the current sale (shares involved in 
            older sales are subtracted from dataframe)
        """
        buy_df = self.df.filter((pl.col("id_order").is_in(self.buy_orders)) & (pl.col("action") == "buy"))

        for row in self.df_older_sales.select("id_order").iter_rows(named=True):
            sale_df = self.df.filter(pl.col("id_order") == row["id_order"])
            row_shares_sold = sale_df.filter(pl.col("action") == "sell").select("number")

            # Compute buys of current row
            buy_df_affected = (
                buy_df.sort("value_date")
                .with_columns(pl.cumsum("number").sub(row_shares_sold).sub(pl.col("number")).alias("pending"))
                .filter(pl.col("pending") <= 0)
                .with_columns(
                    pl.when(abs(pl.col("pending")) > pl.col("number"))
                    .then(pl.col("number"))
                    .otherwise(abs(pl.col("pending")))
                    .alias("shares_effective"))
                .with_columns(
                    (pl.col("number") - pl.col("shares_effective")).alias("number"))
            ).select(self.df.columns)

            # Fitler buys of other sales
            buy_df_untouched = (
                buy_df.sort("value_date")
                .with_columns(
                    pl.cumsum("number").sub(row_shares_sold).sub(pl.col("number")).alias("pending"))
                .filter(pl.col("pending") > 0)
            ).select(self.df.columns)

            # Update buy_df
            buy_df = pl.concat([buy_df_affected, buy_df_untouched])
        return buy_df

    @property
    def buy_orders(self) -> Series:
        """Compute the id of all the purchase id_order involved in transaction

        Returns
        -------
        Series
            contains all the purchase id_order of the stock
        """
        return (
            self.df.
            filter(
                (pl.col("action") == "buy")
            )
            .select("id_order")
            .to_series()
        )
    
    @property
    def shares_purchased(self) -> float: 
        """Get total number of shares purchased in transaction

        Returns
        -------
        float
            total number of shares purchased
        """
        return (
            self.purchase_df.
            filter(
                (pl.col("action") == "buy") 
            )
            .select(pl.sum("shares_effective"))
            .item()
        )
    
    @property
    def purchase_df(self) -> DataFrame:
        """Computes the main dataframe with all purchase information
            -> shares_effective show the shares purchased according to 
                FIFO method for that particular transaction

        Returns
        -------
        DataFrame
            contains all relevant purchase data for a given transaction
        """
        return (
            self.__raw_purchase_df
            .select(pl.all().exclude("number"))
            .join(
                self.__aux_purchase_df.select(["id_order", "shares_effective", "number"]), on="id_order", how="inner"
            )
        )

    
    def aux_purchase_df(self) -> DataFrame:
        return (
            self.purchase_df_after_prev_sales
            .sort("value_date")
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
                .is_in(self.__aux_purchase_df.select("id_order").to_series())
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
        for row in df.filter(pl.col("action") == "sell").select("id_order").iter_rows(named=True):
            sale_df = df.filter(pl.col("id_order") == row["id_order"])

            # add column of possible two month limit restriction
            date_sale = sale_df.filter(pl.col("action") == "sell").select(pl.col("value_date")).item()
            date_2m_limit = date_sale + timedelta(days=60)
            sale_df = df.with_columns(
                pl.when((pl.col("value_date") < date_2m_limit) & (pl.col("value_date") > date_sale))
                .then(False)
                .otherwise(True)
                .alias("two_month_violation")
            ).filter(pl.col("id_order") == row["id_order"])

            # compute sell
            auxsell_df = df.filter((pl.col("id_order") == row["id_order"]) & (pl.col("action") == "sell")).with_columns(
                pl.col("number").alias("shares_effective")
            )
            sale_df = sale_df.select(pl.all().exclude("number")).join(
                auxsell_df.select(["id_order", "number", "shares_effective"]), on="id_order", how="outer"
            )
            shares_sold = df.filter(pl.col("id_order") == row["id_order"]).select(pl.sum("number"))
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
