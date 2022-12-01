from concurrent.futures import ThreadPoolExecutor

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
                print(f"Processing {row['id']}")
                executor.submit(self._update_row, row, columns, "comments")
