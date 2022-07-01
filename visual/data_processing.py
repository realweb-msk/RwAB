import pandas as pd
import numpy as np


def process_tbl_for_dropdown(df: pd.DataFrame, dropdown_col, filter_value):
    """Process changes for backlog table made with slider"""

    # If dropdown with single value
    if isinstance(dropdown_col, str):
        _df = df[df[dropdown_col] == filter_value]

    # If multiple values
    elif isinstance(dropdown_col, tuple([list, tuple, set, dict])):
        _df = df[df[dropdown_col].isin(filter_value)]

    else:
        raise ValueError(f"Filter value must be either str, or iterable, got {type(filter_value)}")

    return _df


def filter_tbl_tab_2(df, _test_name, _start_date, _end_date, _device_type, _user_type):
    # Filter date range and test name
    filtered_df = process_tbl_for_dropdown(
        process_table_date_range(df, _start_date, _end_date),
        dropdown_col="name",
        filter_value=_test_name
    )
    # Filter device_type
    filtered_df = process_tbl_for_dropdown(
        filtered_df,
        dropdown_col="deviceCategory",
        filter_value=_device_type
    )
    # Filter user_type
    filtered_df = process_tbl_for_dropdown(
        filtered_df,
        dropdown_col="userType",
        filter_value=_user_type
    )

    return filtered_df


def filter_tbl_tab_3(df, _test_name, _test_type, _test_segment_0, _test_segment_1, _test_metric):
    # Filter test name
    filtered_df = process_tbl_for_dropdown(
        df,
        dropdown_col="name",
        filter_value=_test_name
    )

    # Filter first group
    filtered_df = process_tbl_for_dropdown(
        filtered_df,
        dropdown_col="group_0",
        filter_value=_test_segment_0
    )

    # Filter second group
    filtered_df = process_tbl_for_dropdown(
        filtered_df,
        dropdown_col="group_1",
        filter_value=_test_segment_1
    )

    # Filter test type
    filtered_df = process_tbl_for_dropdown(
        filtered_df,
        dropdown_col="test_type",
        filter_value=_test_type
    )

    # Filter metric
    filtered_df = process_tbl_for_dropdown(
        filtered_df,
        dropdown_col="metric",
        filter_value=_test_metric
    )

    return filtered_df


def process_table_date_range(df: pd.DataFrame, start_date, end_date, date_col="date"):
    _df = df[df[date_col].between(start_date, end_date)]
    return _df


def get_df_columns_and_uniques(df: pd.DataFrame) -> dict:
    _cols = df.columns
    res = {col: list(df[col].unique()) for col in _cols}

    return res


def process_stat_res_table(res_df: pd.DataFrame, stat_res_df: pd.DataFrame, backlog_df) -> pd.DataFrame:
    stat_res_df = stat_res_df.merge(backlog_df[["name", "experimentId"]], how="inner",
                          left_on="experimentId", right_on="experimentId")

    # Get max and min date from res_df
    _for_join = res_df.groupby("experimentId", as_index=False).agg({"date": ["min", "max"]})
    _for_join.columns = ["experimentId", "min_date", "max_date"]

    stat_res_df = stat_res_df.merge(_for_join, how="inner", left_on="experimentId", right_on="experimentId")

    return stat_res_df


def get_df_sums(df: pd.DataFrame, cols) -> dict:
    """Computes sum of numeric values in provided columns"""
    res = {}
    for col in cols:
        res[col] = df[col].sum()

    return res
