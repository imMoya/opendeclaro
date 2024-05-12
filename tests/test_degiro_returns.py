import numpy as np
import polars as pl
import pytest

from opendeclaro.degiro.returns import Returns


@pytest.fixture
def data_stock():
    dtypes_data_stock = [
        pl.Datetime(time_unit="us", time_zone=None),
        pl.Datetime(time_unit="us", time_zone=None),
        pl.Datetime(time_unit="us", time_zone=None),
        pl.Utf8,
        pl.Utf8,
        pl.Utf8,
        pl.Utf8,
        pl.Float32,
        pl.Utf8,
        pl.Float32,
        pl.Utf8,
        pl.Datetime(time_unit="us", time_zone=None),
        pl.Utf8,
        pl.Float64,
        pl.Float64,
        pl.Utf8,
        pl.Boolean,
        pl.Utf8,
        pl.Float32,
        pl.Float32,
    ]
    return pl.read_csv("opendeclaro/datasets/Account_stocks.csv", dtypes=dtypes_data_stock)


def test_return_on_stock_simple(data_stock):
    assert np.allclose(Returns(data_stock, end_date="01/01/2024").return_on_stock("DE000A3H2200"), 575.927)
