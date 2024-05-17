import polars as pl
from polars import DataFrame


# fmt:off
class DataPrep:
    def __init__(self, data: DataFrame):
        self.data = data
        
    def prepare_id_orders(self):
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
                (pl.col("curr_rate").is_not_null()) &
                (pl.col("id_order").str.lengths() > 0)
            )
            .select(
                "id_order", "curr_rate"
            )
        )
        df.write_csv("datasets/Account_data.csv")
        df_costs.write_csv("datasets/Account_costs.csv")
        df_curr_rate.write_csv("datasets/Account_curr_rate.csv")
        return df.join(df_costs, on="id_order").join(df_curr_rate, left_on="id_order", right_on="id_order", how="left").sort("date", descending=True)
    
    def prepare_involuntary_orders(self):
        df = (
            self.data
            .filter(
                (pl.col("action").str.lengths() > 0) & 
                (pl.col("id_order").str.lengths() == 0)
            )
            .with_columns(pl.lit(None).alias("commision").cast(pl.Float32, strict=False))
        )
        return df.sort("date", descending=True)
    
    @property
    def stocks_orders(self):
        return self.map_eur_curr_rate(
            pl.concat(
                [self.prepare_id_orders(),
                self.prepare_involuntary_orders().select(self.prepare_id_orders().columns)],
                how="align"
            ).sort("date", descending=True)
        )

    
    @staticmethod
    def map_eur_curr_rate(df: DataFrame) -> DataFrame:
        return df.with_columns(
            pl.when(pl.col("varcur").str.contains("EUR"))
            .then(1.0)
            .otherwise(pl.col("curr_rate"))
            .alias("curr_rate")
        )


# fmt:on
