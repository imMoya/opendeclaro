import json
import os
import random
import shutil
import string
from dataclasses import dataclass

from opendeclaro import degiro


@dataclass
class ReturnsISINGlob:
    isin_summary: json
    global_result: float


def returns_from_csv(data_path: str) -> ReturnsISINGlob:
    data = degiro.Dataset(data_path).data
    data_stock = degiro.DataPrep(data).stocks_orders
    degiro_returns = degiro.Returns(data_stock, start_date="01/01/2023", end_date="01/01/2024").return_on_all_stocks()
    isin_summary = degiro_returns.isin_summary.write_json(row_oriented=True)
    global_result = degiro_returns.global_return
    return ReturnsISINGlob(isin_summary, global_result)


def generate_random_str(k: str = 10) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=k))


def create_user_upload_folder(path: str) -> None:
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)
