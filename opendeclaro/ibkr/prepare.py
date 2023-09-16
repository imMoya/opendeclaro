"""prepare.py classes and functions for ibkr"""
import csv

import ibkr.config as config
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

    @staticmethod
    def extract_table(file: str, table_name: str, table_dict: dict = config.table_dict) -> DataFrame:
        """Extract table from IBKR summary .csv

        Parameters
        ----------
        file : str
            path location of dataset csv
        table_name : str
            name of the table to extract (it should be contained in table_dict keys)
        table_dict : dict, optional
            dictionary containing name of the tables inside file

        Returns
        -------
        DataFrame
            polars DataFrame with table values

        Raises
        ------
        ValueError
            "table_name should be contained in table_dict key values"
        """
        for idx, key in enumerate(table_dict):
            if table_name == key:
                table_id = idx
        if table_id == None:
            raise ValueError("table_name should be contained in table_dict key values")

        tables = []
        current_table = None

        with open(file, "r") as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                if len(row) > 1 and row[1] == "Header":
                    current_table = [row]
                    if current_table:
                        tables.append(current_table)
                elif current_table:
                    current_table.append(row)

        if current_table:
            tables.append(current_table)

        df = pl.DataFrame(tables[table_id][1:], schema=tables[table_id][0])

        return df
