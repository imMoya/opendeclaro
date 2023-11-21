import json

from opendeclaro import degiro


def process_csv(data_path: str) -> str:
    deg_data = degiro.Dataset(data_path)
    deg_portfolio = degiro.Portfolio(deg_data.data)
    data = []
    for row in deg_portfolio.stock_sales.iter_rows(named=True):
        item = {"id_order": row["id_order"]}
        ret = deg_portfolio.return_of_sale(deg_data, row["product"], row["id_order"])
        item["name"] = row["product"]
        item["return"] = ret.return_value
        item["two_month_violation"] = ret.two_month_violation
        data.append(item)

    return json.dumps(data, indent=2)
