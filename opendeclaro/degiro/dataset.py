"""prepare.py classes and functions for degiro"""
from typing import List, Union

import polars as pl
from polars import DataFrame, LazyFrame

import opendeclaro.degiro.config as config


class Dataset:
    def __init__(self, path: str):
        """Initialise class

        Parameters
        ----------
        path : str
            path location of dataset csv
        """
        self.data = pl.scan_csv(path)
        self.data_cols = dict(zip(config.cols_list, self.data.columns))
        self.data = self.create_combined_date()
        self.data = self.type_converter()
        self.data = self.split_description()
        self.data = self.data.rename({v: k for k, v in self.data_cols.items()})
        # After handling orphan rows -> EAGER MODE ON
        self.data = self.handle_orphan_rows()
        self.data = self.merge_slot_transaction(action="buy")
        self.data = self.merge_slot_transaction(action="sell")
        self.data = self.category_addition()

    @property
    def change_isin(self) -> dict:
        """Filters dataframe to get the pairs of stocks that changed isin

        Returns
        -------
        list
            dict containing pair of stocks names that changed isin (new isin in key, old in value)
        """
        change_isin_str = "CAMBIO DE ISIN"
        change_isin_df = self.data.filter(pl.col("desc").str.contains(change_isin_str))
        change_isin_list = list(
            change_isin_df.group_by("value_date").agg("product").select("product").to_dict(as_series=False).values()
        )[0]
        return {key: value for key, value in change_isin_list}

    def create_combined_date(self) -> Union[DataFrame, LazyFrame]:
        """Add date column combining value_date and reg_hour"""
        self.data_cols.update(dict(zip(["date"], ["date"])))
        return self.data.with_columns(
            pl.concat_str([self.data_cols["value_date"], self.data_cols["reg_hour"]], separator=" ").alias("date")
        )

    def type_converter(self) -> Union[DataFrame, LazyFrame]:
        """Convert types of columns to appropiate format"""
        return self.data.select(
            pl.col(self.data_cols["reg_date"]).str.strptime(pl.Datetime),
            pl.col(self.data_cols["reg_hour"]).str.strptime(pl.Datetime, "%H:%M"),
            pl.col(self.data_cols["value_date"]).str.strptime(pl.Datetime),
            pl.col("date").str.strptime(pl.Datetime),
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

    def drop_orphan_rows(self) -> Union[DataFrame, LazyFrame]:
        """Drop orphan rows where no date data is available"""
        return self.data.filter(pl.col(self.data_cols["reg_date"]) != None)

    def handle_orphan_rows(self) -> Union[DataFrame, LazyFrame]:
        """Handle orphan rows where no date data is available.

        When an operation is divided into two rows (due to broker mistakes), this leads
        to what we call an 'orphan row', which is the second row related to the operation.
        This method merges the 'orphan row' (only the str columns )with the 'mother row'
        (the row above)

        Returns
        -------
        Union[DataFrame, LazyFrame]
            which contains no 'orphan rows' in it
        """
        self.data = self.data.collect()
        orphan_data = self.replace_null_str(self.data.with_row_count().filter(pl.col("reg_date").is_null()))
        mapping = {i: i - 1 for i in orphan_data.select(pl.col("row_nr")).to_series()}
        list_del = [i for i in orphan_data.select(pl.col("row_nr")).to_series()]
        orphan_data = orphan_data.with_columns(pl.col("row_nr").map_dict(mapping))
        mother_data = self.replace_null_str(self.data.with_row_count().join(orphan_data, on="row_nr", how="left"))

        for col_str in self.data.select(pl.col(pl.Utf8)).columns:
            mother_data = mother_data.with_columns(
                pl.concat_str([col for col in mother_data.columns if col_str in col], separator="").alias(col_str)
            )
        mother_data = mother_data.filter(~pl.col("row_nr").is_in(list_del))
        return self.replace_str_null(mother_data[list(self.data_cols.keys())])

    def merge_slot_transaction(self, action: str = "buy") -> DataFrame:
        unique_buy = (
            (
                self.data.filter(pl.col("action") == action)
                .group_by("id_order", maintain_order=True)
                .agg(
                    pl.col("reg_date").unique().alias("reg_date_list"),
                    pl.col("reg_hour").unique().alias("reg_hour_list"),
                    pl.col("value_date").unique().alias("value_date_list"),
                    pl.col("date").unique().alias("date_list"),
                    pl.col("product").unique().alias("product_list"),
                    pl.col("isin").unique().alias("isin_list"),
                    pl.col("desc").unique().alias("desc_list"),
                    pl.col("curr_rate").unique().alias("curr_rate_list"),
                    pl.col("varcur").unique().alias("varcur_list"),
                    pl.col("var").sum(),
                    pl.col("cashcur").unique().alias("cashcur_list"),
                    pl.col("cash").sum(),
                    pl.col("number").sum(),
                    pl.col("price").mean(),
                    pl.col("pricecur").unique().alias("pricecur_list"),
                )
            )
            .with_columns(
                pl.col("reg_date_list").map_elements(lambda x: x[0]).alias("reg_date"),
                pl.col("reg_hour_list").map_elements(lambda x: x[0]).alias("reg_hour"),
                pl.col("value_date_list").map_elements(lambda x: x[0]).alias("value_date"),
                pl.col("date_list").map_elements(lambda x: x[0]).alias("date"),
                pl.col("product_list").map_elements(lambda x: x[0]).alias("product"),
                pl.col("isin_list").map_elements(lambda x: x[0]).alias("isin"),
                pl.col("desc_list").map_elements(lambda x: x[0]).alias("desc"),
                pl.col("curr_rate_list").map_elements(lambda x: x[0]).alias("curr_rate"),
                pl.col("varcur_list").map_elements(lambda x: x[0]).alias("varcur"),
                pl.col("cashcur_list").map_elements(lambda x: x[0]).alias("cashcur"),
                pl.lit(action).alias("action"),
                pl.col("pricecur_list").map_elements(lambda x: x[0]).alias("pricecur"),
            )
            .select(self.data.columns)
        )
        return pl.concat([self.data.filter(pl.col("action") != action), unique_buy], how="diagonal").sort(
            "value_date", descending=True
        )

    def split_description(self) -> Union[DataFrame, LazyFrame]:
        """Apply split_and_transform method to description column of data and update data_cols with new column names"""
        self.data_cols.update(
            dict(zip(["action", "number", "price", "pricecur"], ["action", "number", "price", "pricecur"]))
        )
        return self.data.with_columns(
            pl.col(self.data_cols["desc"]).map_elements(self.split_and_transform).alias("result")
        ).unnest("result")

    def category_addition(self) -> Union[DataFrame, LazyFrame]:
        return self.data.with_columns(
            category=pl.when(pl.col("desc").str.contains("|".join(self.options_names())))
            .then("option")
            .when((pl.col("action") == "buy") | (pl.col("action") == "sell"))
            .then("stock")
            .when(pl.col("desc") == "Dividendo")
            .then("dividend")
        )

    @staticmethod
    def split_and_transform(desc: str) -> dict:
        """Split string and get dictionary with four items: action, number, price and pricecur

        Parameters
        ----------
        desc : str
            string of description (associated to description column)

        Returns
        -------
        dict
            dictionary containing action, number, price and price currency
        """
        mapping = {"Compra": "buy", "Venta": "sell"}
        if desc.startswith("Compra") or desc.startswith("Venta"):
            split_row = desc.split("@")
            return {
                "action": mapping.get(split_row[0].split()[0]),
                "number": float(split_row[0].split()[1]),
                "price": float(split_row[1].split()[0].replace(",", ".")),
                "pricecur": split_row[1].split()[1],
            }

        elif desc.startswith("ESCISI"):
            _desc = desc.split(": ")[1]
            split_row = _desc.split("@")
            return {
                "action": mapping.get(split_row[0].split()[0]),
                "number": float(split_row[0].split()[1]),
                "price": float(split_row[1].split()[0].replace(",", ".")),
                "pricecur": split_row[1].split()[1],
            }

        elif desc.startswith("VENCIMIENTO"):
            split_row = desc.split(": ")[1].split("@")
            return {
                "action": mapping.get(split_row[0].split()[0]),
                "number": float(split_row[0].split()[1]),
                "price": float(split_row[1].split()[0].replace(",", ".")),
                "pricecur": split_row[1].split()[1],
            }
        else:
            return {
                "action": None,
                "number": None,
                "price": None,
                "pricecur": None,
            }

    @staticmethod
    def replace_null_str(df: Union[DataFrame, LazyFrame]) -> Union[DataFrame, LazyFrame]:
        return df.with_columns(
            pl.when(pl.col(pl.Utf8).is_null()).then(pl.lit("")).otherwise(pl.col(pl.Utf8)).keep_name()
        )

    @staticmethod
    def replace_str_null(df: Union[DataFrame, LazyFrame]) -> Union[DataFrame, LazyFrame]:
        return df.with_columns(
            pl.when(pl.col(pl.Utf8) == " ").then(None).otherwise(pl.col(pl.Utf8)).keep_name()  # keep original value
        )

    @staticmethod
    def options_names() -> List[str]:
        return ["JAN2", "FEB2", "MAR2", "APR2", "JUN2", "JUL2", "AUG2", "SEP2", "OCT2", "NOV2", "DEC2"]
