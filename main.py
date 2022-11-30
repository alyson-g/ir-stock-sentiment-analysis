import os

from dotenv import load_dotenv
import praw
from psaw import PushshiftAPI
from psycopg2.pool import ThreadedConnectionPool

from comment_extractor import CommentExtractor
from post_extractor import PostExtractor

load_dotenv()


def main():
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
    extractor.find_all_posts("2020-12-01", "2022-11-27")

    extractor = CommentExtractor(api, pool)
    extractor.find_all()


if __name__ == "__main__":
    main()
