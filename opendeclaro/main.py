from typing import Optional

from opendeclaro.degiro.dataprep import DataPrep
from opendeclaro.degiro.dataset import Dataset

if __name__ == "__main__":
    data = Dataset("datasets/Account.csv").data
    DataPrep(data).prepare_stock_id_orders().write_csv("datasets/Account_postprocessed.csv")
