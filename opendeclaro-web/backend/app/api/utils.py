import json
import os
import random
import shutil
import string

from opendeclaro import degiro


def returns_from_csv(data_path: str) -> str:
    data = degiro.Dataset(data_path).data
    data_stock = degiro.DataPrep(data).stocks_orders
    isin_summary = (
        degiro.Returns(data_stock, start_date="01/01/2023", end_date="01/01/2024").return_on_all_stocks().isin_summary
    )
    return isin_summary.write_json(row_oriented=True)


def generate_random_str(k: str = 10) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=k))


def create_user_upload_folder(path: str) -> None:
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)
