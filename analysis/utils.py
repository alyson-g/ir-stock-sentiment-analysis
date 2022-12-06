from datetime import datetime

import pandas as pd


def get_date_range(
        data: pd.DataFrame,
        start_date: str,
        end_date: str,
        date_column: str
) -> pd.DataFrame:
    """Modify stock DataFrame to only include certain dates.

    :param data: A DataFrame of historical stock data
    :param start_date: The start date of the range in the form YYYY-MM-DD
    :param end_date: The end date of the range in the form YYYY-MM-DD
    :param date_column: The name of the date column
    :return: The modified DataFrame of stock data
    """
    start = datetime.fromisoformat(start_date).date()
    end = datetime.fromisoformat(end_date).date()

    data_range = data[data[date_column] >= start]
    return data_range[data_range[date_column] < end]


def merge_stocks_and_posts(stocks: pd.DataFrame, posts: pd.DataFrame) -> pd.DataFrame:
    """Merge DataFrames of stock and post data.

    :param stocks: A DataFrame of historical stock data
    :param posts: A DataFrame of posts
    :return: A merged DataFrame
    """
    merged = posts.merge(stocks, how="left", left_on="created_date", right_on="Date")
    merged = merged.drop(["Date"], axis=1)
    return merged
