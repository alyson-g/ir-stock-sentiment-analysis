from concurrent.futures.thread import ThreadPoolExecutor
import logging


from sentiment.sentiment_base import SentimentBase


class PostAdder(SentimentBase):
    """Adds sentiment scores to posts stored in a Postgres database."""

    def apply_sentiment(self):
        """Apply sentiment values to posts stored in the database."""
        query = "SELECT id, title, post_text FROM posts WHERE vader_score_title IS NULL ORDER BY created_at ASC"

        data = self._retrieve_data(query)
        columns = ["title", "post_text"]

        with ThreadPoolExecutor() as executor:
            for _, row in data.iterrows():
                logging.info(f"Processing {row['id']}")
                executor.submit(self._update_row, row, columns, "posts")
