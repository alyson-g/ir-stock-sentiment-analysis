from typing import List

import pandas as pd


def get_data(symbols: List[str], window: int = 5) -> pd.DataFrame:
    """Prepare stock data for analysis.

    :param symbols: The list of stock symbols to prepare data for
    :param window: The size of the window over which to aggregate data
    :return: A DataFrame of historical stock information
    """
    data = pd.DataFrame()

    for symbol in symbols:
        file_name = f"./data/{symbol.lower()}_us_d.csv"
        intermediate_data = pd.read_csv(file_name)
        intermediate_data["symbol"] = symbol
        data = pd.concat([data, intermediate_data])

    data["Date"] = pd.to_datetime(data["Date"]).dt.date

    data["Rolling_Open"] = data[::-1]["Open"].rolling(window).mean()[::-1].shift(-(2 * window))
    data["Rolling_High"] = data[::-1]["High"].rolling(window).mean()[::-1].shift(-(2 * window))
    data["Rolling_Low"] = data[::-1]["Low"].rolling(window).mean()[::-1].shift(-(2 * window))
    data["Rolling_Close"] = data[::-1]["Close"].rolling(window).mean()[::-1].shift(-(2 * window))
    data["Rolling_Volume"] = data[::-1]["Volume"].rolling(window).mean()[::-1].shift(-(2 * window))

    return data
