from concurrent.futures import ThreadPoolExecutor
import logging

from sentiment.sentiment_base import SentimentBase


class CommentAdder(SentimentBase):
    """Adds sentiment scores to comments stored in a Postgres database."""

    def apply_sentiment(self):
        """Apply sentiment values to comments stored in the database."""
        query = "SELECT id, body FROM comments"

        data = self._retrieve_data(query)
        columns = ["body"]

        with ThreadPoolExecutor() as executor:
            for _, row in data.iterrows():
                logging.info(f"Processing {row['id']}")
                executor.submit(self._update_row, row, columns, "comments")
