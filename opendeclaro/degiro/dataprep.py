import polars as pl
from polars import DataFrame


# fmt:off
class DataPrep:
    def __init__(self, data: DataFrame):
        self.data = data

    def prepare_stock_id_orders(self):
        df = (
            self.data
            .filter(
                (pl.col("id_order").str.lengths() > 0) & 
                (pl.col("action").str.lengths() > 0)
            )
            .select(pl.exclude("curr_rate"))
        )
        df_costs = (
            self.data
            .filter(
                (pl.col("id_order").str.lengths() > 0) &
                (pl.col("action").str.lengths() == 0) & 
                (pl.col("desc").str.contains("Divisa")).not_()
            )
            .group_by("id_order")
            .agg(pl.col("var").sum().alias("commision"))
        )
        df_curr_rate = (
            self.data
            .filter(
                pl.col("curr_rate").str.lengths() > 0 
            )
            .select(
                "id_order", "curr_rate"
            )
        )
        return df.join(df_costs, on="id_order").join(df_curr_rate, on="id_order").filter(pl.col("category") == "stock").unique(maintain_order=True)
# fmt:on
