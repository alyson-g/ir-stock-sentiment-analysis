from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from itertools import pairwise
from typing import Generator, Optional

from praw.models import Submission

from extractor_base import ExtractorBase


class PostExtractor(ExtractorBase):
    """Extracts posts using PSAW and PRAW."""

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
        print(f"Processing {datetime.utcfromtimestamp(start_date).strftime('%Y-%m-%d')}...")

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
