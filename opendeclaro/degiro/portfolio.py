from datetime import datetime
from typing import Optional

import polars as pl
from degiro.dataset import Dataset
from degiro.stocks import PurchaseOfStockFromSale, SaleOfStock, Stocks
from polars import DataFrame


class Portfolio:
    def __init__(self, df: DataFrame, year: Optional[int] = None):
        self.df = df.sort(pl.col("value_date"), descending=True)
        self.year = year

    @property
    def stock_sales(self) -> DataFrame:
        return self.get_stock_sales()

    def get_stock_sales(self) -> DataFrame:
        if self.year == None:
            return self.df.filter(pl.col("action") == "sell").select(["product", "id_order"])
        else:
            return self.df.filter(
                (pl.col("value_date") >= datetime(self.year, 1, 1)) & (pl.col("action") == "sell")
            ).select(["product", "id_order"])
