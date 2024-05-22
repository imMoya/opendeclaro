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


def test_return_on_stock_one_sale_one_buy_eur(data_stock):
    assert np.allclose(
        Returns(data_stock, start_date="01/01/2023", end_date="01/01/2024").return_on_stock("IE00BD8PGZ49"), 70.85
    )


def test_return_on_stock_one_sale_two_buy_eur(data_stock):
    assert np.allclose(
        Returns(data_stock, start_date="01/01/2023", end_date="01/01/2024").return_on_stock("ES0105546008"), -283.47
    )


def test_return_on_stock_one_sale_one_buy_usd(data_stock):
    assert np.allclose(
        Returns(data_stock, start_date="01/01/2023", end_date="01/01/2024").return_on_stock("BMG9456A1009"), -318.85
    )


def test_return_on_stock_change_isin(data_stock):
    assert np.allclose(
        Returns(data_stock, start_date="01/01/2023", end_date="01/01/2024").return_on_stock("CA11271J1075"), -521.929
    )


def test_return_on_stock_spinoff(data_stock):
    # TODO: Add assertion to test that the buy of spinoff has null value
    assert np.allclose(
        Returns(data_stock, start_date="01/01/2023", end_date="01/01/2024").return_on_stock("CA1130041058"), 209.18
    )


def test_return_on_stock_fifo(data_stock):
    assert np.allclose(
        Returns(data_stock, start_date="01/01/2023", end_date="01/01/2024").return_on_stock("DE000A3H2200"), 575.927
    )


def test_one_year_return(data_stock):
    assert np.allclose(
        Returns(data_stock, start_date="01/01/2023", end_date="01/01/2024").return_on_all_stocks().global_return,
        1367.929,
    )
