from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import polars as pl
from polars import DataFrame

from opendeclaro.degiro.dataset import Dataset
from opendeclaro.degiro.stocks import PurchaseOfStockFromSale, SaleOfStock, Stocks


@dataclass
class Return:
    return_value: float
    two_month_violation: bool


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

    @staticmethod
    def return_of_sale(ds: Dataset, product: str, id_order: str) -> Return:
        """Computes the return of a sale.
        1. Initialize the SaleOfStock class
        2. Initialize the PurchaseOfStockFromSale class
        3. If shares sold are larger that shares purchased it assumes that there has been a change in isin
        4. Appends shares pre-isin and post-isin change
        5. Computes return of sale taking into account the shares effective to the sale


        Parameters
        ----------
        ds : Dataset
            object associated to degiro dataset treated
        product : str
            name of financial product associated to the return computation
        id_order : str
            id_order associated to the sale

        Returns
        -------
        float

        """
        sos = SaleOfStock(ds, product, id_order)
        sale_df = sos.sale_df
        pos = PurchaseOfStockFromSale(ds, product, id_order)
        buy_df = pos.purchase_df
        if sos.shares_sold > pos.shares_purchased:
            _pos = PurchaseOfStockFromSale(ds, product, id_order, change_isin=True)
            buy_df = pl.concat([buy_df, _pos.purchase_df])
            total_purchased = pos.shares_purchased + _pos.shares_purchased
            assert sos.shares_sold == total_purchased
        all_df = pl.concat([sale_df, buy_df], how="diagonal").filter(pl.col("shares_effective") != 0)
        return_sale = all_df.select((pl.col("var") * pl.col("shares_effective") / pl.col("number"))).sum().item()
        two_month_violation = (
            True if (return_sale < 0) & (sale_df.select("two_month_violation")[0].item() == True) else False
        )
        return Return(return_value=return_sale, two_month_violation=two_month_violation)
