from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Generator, List

from praw.models import Comment

from extractor.extractor_base import ExtractorBase


class CommentExtractor(ExtractorBase):
    """Extracts comments using PSAW and PRAW."""

    def _get_post_ids(self) -> List[str]:
        """Retrieve all post IDs stored in the database."""
        conn = self.pool.getconn()
        cur = conn.cursor()

        query = "SELECT DISTINCT id FROM posts"
        cur.execute(query)

        results = [result[0] for result in cur.fetchall()]

        self.pool.putconn(conn)
        return results

    def _get_generator(
            self,
            post_id: str,
            subreddit: str
    ) -> Generator[Comment, None, None]:
        """Gets a comment generator for a specified subreddit.

        :param post_id: The ID of the parent post
        :param subreddit: The name of the subreddit to search
        :return: A generator of comments
        """
        return self.api.search_comments(
            link_id=post_id,
            subreddit=subreddit,
            filter=[
                "id",
                "author",
                "body",
                "created_utc"
            ]
        )

    def _finder_process(self, post_id: str, subreddit: str) -> None:
        """Extract the comments for the specified post.

        :param post_id: The ID of the post
        :return: None
        """
        print(f"Processing post {post_id}...")

        generator = self._get_generator(post_id, subreddit)
        comments = list(generator)

        conn = self.pool.getconn()
        for comment in comments:
            comment_id = comment.id
            body = comment.body
            author = comment.author.name if comment.author else None
            created_at = datetime.utcfromtimestamp(comment.created_utc)

            query = """
                    INSERT INTO comments
                        (id, parent_id, body, author, created_at)
                    VALUES
                        (%s, %s, %s, %s, %s)
                    """
            cur = conn.cursor()
            cur.execute(query, (comment_id, post_id, body, author, created_at))

            conn.commit()

        self.pool.putconn(conn)

    def find_all(
            self,
            subreddit: str = "wallstreetbets"
    ) -> None:
        """Find all comments and insert them in a Postgres database.

        :param subreddit: The name of the subreddit to search
        :return: None
        """
        posts = self._get_post_ids()

        with ThreadPoolExecutor() as executor:
            for post in posts:
                executor.submit(
                    self._finder_process,
                    post,
                    subreddit
                )
