import os
from typing import List

import pandas as pd
import psycopg2


def pg_conn():
    """Get a Postgres connection."""
    PG_USER = os.getenv("PG_USER")
    PG_PASSWORD = os.getenv("PG_PASSWORD")
    PG_DATABASE = os.getenv("PG_DATABASE")
    return psycopg2.connect(user=PG_USER, password=PG_PASSWORD, database=PG_DATABASE)


def get_data(symbols: List[str]) -> pd.DataFrame:
    """Get data for a particular stock symbol.

    :param symbols: The list of stock symbols to grab data for
    :return: A DataFrame of post information
    """
    query = """
            SELECT 
                posts.id, 
                author,
                created_at, 
                vader_score_title,
                roberta_score_title,
                vader_score_post_text,
                roberta_score_post_text
            FROM posts
            INNER JOIN posts_stocks_xref AS xr
                ON posts.id = xr.id
            WHERE xr.symbol IN %s
            """
    conn = pg_conn()
    cur = conn.cursor()
    cur.execute(query, (tuple(symbols), ))

    col_names = [desc[0] for desc in cur.description]
    results = cur.fetchall()

    data = pd.DataFrame(results, columns=col_names)
    data["created_date"] = pd.to_datetime(data["created_at"]).dt.date

    return data


def get_total_posts_per_day() -> pd.DataFrame:
    """Get the total number of posts per day."""
    query = """
            SELECT 
                COUNT(id) as total_num_posts, 
                created_at::date AS created_date
            FROM posts
            GROUP BY created_date
            """

    conn = pg_conn()
    cur = conn.cursor()
    cur.execute(query)

    col_names = [desc[0] for desc in cur.description]
    results = cur.fetchall()

    return pd.DataFrame(results, columns=col_names)


def prepare_sentiment_scores(
        data: pd.DataFrame,
        score_column: str
) -> pd.DataFrame:
    """Prepare sentiment scores for analysis.

    :param data: A DataFrame of post data
    :param score_column: The name of the column containing sentiment scores
    :return: A DataFrame containing prepared sentiment scores
    """
    data[score_column] = data[score_column].astype("category")
    data[score_column] = data[score_column].replace(
        to_replace=["negative", "neutral", "positive"], value=[-1, 0, 1]
    )
    data[score_column] = data[score_column].astype("float")
    data = data.dropna()

    return data


def aggregate(
        data: pd.DataFrame,
        agg_column: str,
        how: str = "mean"
) -> pd.DataFrame:
    """Aggregate data across dates.

    :param data: A DataFrame of stock and post data
    :param agg_column: The name of the column to aggregate over
    :param how: How to aggregate, either count or mean
    :return: An aggregated DataFrame
    """
    if how == "count":
        if agg_column == "id":
            return data.groupby(
                ["created_date"],
                as_index=False
            )["id"].agg("count")
        else:
            return data.groupby(
                ["created_date", agg_column],
                as_index=False
            )["id"].agg("count")
    else:
        return data.groupby(
            ["created_date"],
            as_index=False
        )[agg_column].mean()
