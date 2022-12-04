from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, timedelta
from itertools import pairwise
import logging
from time import sleep
from typing import Generator, Optional, List

from psaw import PushshiftAPI
from praw.models import Submission
from psycopg2.pool import ThreadedConnectionPool


class PostExtractor:
    """Extracts posts using PSAW and PRAW."""

    def __init__(self, api: PushshiftAPI, pool: ThreadedConnectionPool):
        """Initialize a PostExtractor instance.

        :param api: An initialized PushshiftAPI client
        :param pool: An initialized thread-safe Postgres connection pool
        """
        self.api = api
        self.pool = pool

    @staticmethod
    def _str_to_unix_timestamp(date: datetime.date) -> int:
        """Convert a date in a string to a Unix timestamp.

        :param date: A datetime date
        :return: A Unix timestamp
        """
        return int(date.timestamp())

    def _get_all_dates_in_range(
            self,
            start_date: datetime.date,
            end_date: datetime.date
    ) -> List[int]:
        """Return all dates between two dates.

        :param start_date: The start date of the range
        :param end_date: The end date of the range
        :return: A list of dates as Unix timestamps
        """
        delta = end_date - start_date
        days = []

        for i in range(delta.days + 1):
            date = self._str_to_unix_timestamp(start_date + timedelta(days=i))
            days.append(date)

        return days

    def _get_generator(
            self,
            start_date: int,
            end_date: int,
            subreddit: str = "wallstreetbets",
            limit: Optional[int] = None
    ) -> Generator[Submission, None, None]:
        """Gets a post generator for a specified date range.

        :param start_date: The Unix timestamp start date
        :param end_date: The Unix timestamp end date
        :param subreddit: The name of the subreddit to search
        :param limit: The maximum number of posts the API returns
        :return: A generator of posts
        """
        sleep(1)
        return self.api.search_submissions(
            after=start_date,
            before=end_date,
            subreddit=subreddit,
            sort_type="created_utc",
            sort="asc",
            filter=[
                "id",
                "author",
                "title",
                "selftext",
                "num_comments",
                "created_utc"
            ],
            limit=limit
        )

    def _finder_process(
            self,
            start_date: int,
            end_date: int,
            subreddit: str,
            limit: int
    ) -> None:
        """Extract the posts for a specific date.

        :param start_date: The start date as a Unix timestamp
        :param end_date: The end date as a Unix timestamp
        :param subreddit: The name of the subreddit to search
        :param limit: The maximum number of posts to retrieve
        :return: None
        """
        generator = self._get_generator(
            start_date,
            end_date,
            subreddit,
            limit
        )

        for post in generator:
            submission_id = post.id
            title = post.title
            text = post.selftext
            author = post.author.name if post.author else None
            num_comments = post.num_comments
            created_at = datetime.utcfromtimestamp(post.created_utc)

            conn = self.pool.getconn()
            cur = conn.cursor()

            query = """
                        INSERT INTO POSTS
                            (id, title, post_text, author, num_comments, created_at)
                        VALUES
                            (%s, %s, %s, %s, %s, %s)
                        """
            cur.execute(query, (submission_id, title, text, author, num_comments, created_at))
            conn.commit()
            self.pool.putconn(conn)

        logging.info(f"Processed {datetime.utcfromtimestamp(start_date).strftime('%Y-%m-%d')}")

    def find_all(
            self,
            start_date: str,
            end_date: str,
            subreddit: str = "wallstreetbets",
            limit: Optional[int] = None
    ) -> None:
        """Find all posts in a specified time range, and insert it into a Postgres database.

        :param start_date: The start date in the form YYYY-MM-DD
        :param end_date: The end date in the form YYYY-MM-DD
        :param subreddit: The name of the subreddit to search
        :param limit: The maximum number of posts to process
        :return: None
        """
        start_date_dt = datetime.fromisoformat(start_date)
        end_date_dt = datetime.fromisoformat(end_date)

        all_dates = self._get_all_dates_in_range(start_date_dt, end_date_dt)

        with ThreadPoolExecutor() as executor:
            for start, end in pairwise(all_dates):
                executor.submit(
                    self._finder_process,
                    start,
                    end,
                    subreddit,
                    limit
                )
