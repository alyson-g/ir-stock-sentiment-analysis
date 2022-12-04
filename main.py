import os
import logging

from dotenv import load_dotenv
import praw
from psaw import PushshiftAPI
from psycopg2.pool import ThreadedConnectionPool

from extractor.comment_extractor import CommentExtractor
from extractor.post_extractor import PostExtractor
from sentiment.comment_adder import CommentAdder
from sentiment.post_adder import PostAdder

load_dotenv()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s"
    )

    client_id = os.getenv("PRAW_CLIENT_ID")
    client_secret = os.getenv("PRAW_SECRET")
    user_agent = os.getenv("PRAW_USER_AGENT")

    client = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
    )
    api = PushshiftAPI(client)

    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    database = os.getenv("PG_DATABASE")

    pool = ThreadedConnectionPool(
        minconn=1,
        maxconn=10,
        user=user,
        password=password,
        database=database
    )

    extractor = PostExtractor(api, pool)
    extractor.find_all("2020-12-01", "2022-01-01")

    extractor = CommentExtractor(client, pool)
    extractor.find_all()

    post_adder = PostAdder(pool)
    post_adder.apply_sentiment()

    comment_adder = CommentAdder(pool)
    comment_adder.apply_sentiment()


if __name__ == "__main__":
    main()
