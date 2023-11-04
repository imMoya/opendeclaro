"""prepare.py classes and functions for degiro"""
import degiro.config as config
import polars as pl
from polars import DataFrame


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
        self.data = self.type_converter()
        self.data = self.split_description()
        self.data = self.data.rename({v: k for k, v in self.data_cols.items()})
        self.data = self.handle_orphan_rows()

    def type_converter(self) -> DataFrame:
        """Convert types of columns to appropiate format"""
        return self.data.select(
            pl.col(self.data_cols["reg_date"]).str.strptime(pl.Datetime),
            pl.col(self.data_cols["reg_hour"]).str.strptime(pl.Datetime, "%H:%M"),
            pl.col(self.data_cols["value_date"]).str.strptime(pl.Datetime),
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

    def drop_orphan_rows(self) -> DataFrame:
        """Drop orphan rows where no date data is available"""
        return self.data.filter(pl.col(self.data_cols["reg_date"]) != None)

    def handle_orphan_rows(self) -> DataFrame:
        """Handle orphan rows where no date data is available"""
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

    def split_description(self) -> DataFrame:
        """Apply split_and_transform method to description column of data and update data_cols with new column names"""
        self.data_cols.update(
            dict(zip(["action", "number", "price", "pricecur"], ["action", "number", "price", "pricecur"]))
        )
        return self.data.with_columns(
            pl.col(self.data_cols["desc"]).map_elements(self.split_and_transform).alias("result")
        ).unnest("result")

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
        isin_change_str = "CAMBIO DE ISIN: "
        if desc.startswith("Compra") or desc.startswith("Venta"):
            split_row = desc.split("@")
            return {
                "action": mapping.get(split_row[0].split()[0]),
                "number": float(split_row[0].split()[1]),
                "price": float(split_row[1].split()[0].replace(",", ".")),
                "pricecur": split_row[1].split()[1],
            }

        elif desc.startswith(isin_change_str):
            _desc = desc.replace(isin_change_str, "")
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
    def replace_null_str(df: DataFrame) -> DataFrame:
        return df.with_columns(
            pl.when(pl.col(pl.Utf8).is_null()).then(pl.lit("")).otherwise(pl.col(pl.Utf8)).keep_name()
        )

    @staticmethod
    def replace_str_null(df: DataFrame) -> DataFrame:
        return df.with_columns(
            pl.when(pl.col(pl.Utf8) == " ").then(None).otherwise(pl.col(pl.Utf8)).keep_name()  # keep original value
        )
