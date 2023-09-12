"""prepare.py classes and functions"""
from dataclasses import dataclass

import config
import polars as pl


class Dataset:
    def __init__(self, path: str):
        """Initialise class

        Parameters
        ----------
        path : str
            path location of dataset csv
        """
        self.data = pl.read_csv(path)
        self.data_cols = dict(zip(config.cols_list, self.data.columns))

    def type_converter(self):
        """Converts types of columns to appropiate format"""
        out = self.data.select(
            pl.col(self.data_cols["reg_date"]).str.strptime(pl.Datetime),
            pl.col(self.data_cols["reg_hour"]).str.strptime(pl.Datetime, "%H:%M"),
            pl.col(self.data_cols["value_date"]),
            pl.col(self.data_cols["product"]),
            pl.col(self.data_cols["isin"]),
            pl.col(self.data_cols["desc"]),
            pl.col(self.data_cols["curr_rate"]),
            pl.col(self.data_cols["varcur"]),
            pl.col(self.data_cols["var"]).str.replace(",", ".").cast(pl.Float32, strict=False),
            pl.col(self.data_cols["cashcur"]),
            pl.col(self.data_cols["cash"]).str.replace(",", ".").cast(pl.Float32, strict=False),
            pl.col(self.data_cols["id_order"]),
        )
        print(out)
