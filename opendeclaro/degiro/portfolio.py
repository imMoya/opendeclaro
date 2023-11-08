from datetime import datetime
from typing import Optional

import polars as pl
from degiro.dataset import Dataset
from degiro.stocks import PurchaseOfStock, SaleOfStock
from polars import DataFrame


class Portfolio:
    def __init__(self, df: DataFrame, year: Optional[int] = None):
        self.df = df.sort(pl.col("value_date"), descending=True)
        self.year = year

    @property
    def stock_sales(self) -> dict:
        stock_list = list(self.get_stock_sales().values())
        my_dict = {}
        keys = stock_list[0]
        values = stock_list[1]

        for i in range(len(keys)):
            my_dict[keys[i]] = values[i]
        return my_dict

    def get_stock_sales(self) -> dict:
        if self.year == None:
            return self.df.filter(pl.col("action") == "sell").select(["product", "id_order"]).to_dict(as_series=False)
        else:
            return (
                self.df.filter((pl.col("value_date") >= datetime(self.year, 1, 1)) & (pl.col("action") == "sell"))
                .select(["product", "id_order"])
                .to_dict(as_series=False)
            )
