"""This class defined in this file applies sentiment scores to posts stored in a Postgres database.
The sentiment analysis code was modified from code found in the two articles below.

VADER Sentiment Analysis: https://towardsdatascience.com/sentimental-analysis-using-vader-a3415fef7664
roBERTa Sentiment Analysis: https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment
"""
from abc import ABC, abstractmethod
import csv
import logging
from typing import List
import urllib

from nltk.sentiment.vader import SentimentIntensityAnalyzer
import pandas as pd
from psycopg2.pool import ThreadedConnectionPool
from scipy.special import softmax
from transformers import AutoModelForSequenceClassification
from transformers import AutoTokenizer
from transformers.tokenization_utils_base import BatchEncoding


class SentimentBase(ABC):
    def __init__(self, pool: ThreadedConnectionPool):
        """Initialize a SentimentBase.

        :param pool: An initialized thread-safe Postgres connection pool
        """
        self.pool = pool

        self.vader = SentimentIntensityAnalyzer()

        self.model_name = "cardiffnlp/twitter-roberta-base-sentiment"
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
        self.roberta_labels = self._download_roberta_mapping()

    @staticmethod
    def _download_roberta_mapping() -> List[str]:
        """Downloads a list of sentiment labels.

        :return: A list of string labels
        """
        mapping_link = f"https://raw.githubusercontent.com/cardiffnlp/tweeteval/main/datasets/sentiment/mapping.txt"
        with urllib.request.urlopen(mapping_link) as f:
            html = f.read().decode("utf-8").split("\n")
            csv_reader = csv.reader(html, delimiter="\t")

        return [row[1] for row in csv_reader if len(row) > 1]

    def _preprocess(self, text: str) -> BatchEncoding:
        """Preprocess text for use with the roBERTa model.

        :param text: A string to preprocess
        :return: A Pandas Series containing the preprocessed string
        """
        tokens = []
        for token in text.split(" "):
            token = 'http' if token.startswith('http') else token
            tokens.append(token)

        return self.tokenizer(" ".join(tokens), return_tensors="pt")

    def _extract_score(self, encoded_input: BatchEncoding) -> str:
        """Extract the sentiment score from encoded roBERTa data.

        :param encoded_input: An encoded row of data
        :return: The highest probability class
        """
        output = self.model(**encoded_input)
        scores = output[0][0].detach().numpy()
        scores = softmax(scores)

        positive = self.roberta_labels.index("positive")
        negative = self.roberta_labels.index("negative")
        neutral = self.roberta_labels.index("neutral")

        if scores[negative] > scores[positive] and scores[negative] > scores[neutral]:
            return "negative"
        elif scores[positive] > scores[negative] and scores[positive] > scores[neutral]:
            return "positive"
        else:
            return "neutral"

    def _calculate_vader(self, text: str) -> str:
        """Calculate the VADER sentiment score using NLTK.

        :param text: The text to score
        :return: The highest probability class
        """
        scores = self.vader.polarity_scores(text)

        negative = scores["neg"]
        positive = scores["pos"]
        neutral = scores["neu"]

        if negative > positive and negative > neutral:
            return "negative"
        elif positive > negative and positive > neutral:
            return "positive"
        else:
            return "neutral"

    def _calculate_roberta(self, text: str) -> str:
        """Calculate the roBERTA sentiment score using hugging-face transformers.

        :param text: The text to score
        :return: The highest probability class
        """
        encoded_input = self._preprocess(text)
        return self._extract_score(encoded_input)

    def _retrieve_data(self, query: str) -> pd.DataFrame:
        """Retrieves data from the database.

        :param query: The query to execute
        :return: A DataFrame of returned results
        """
        conn = self.pool.getconn()
        cur = conn.cursor()
        cur.execute(query)

        col_names = [desc[0] for desc in cur.description]
        results = cur.fetchall()

        self.pool.putconn(conn)

        return pd.DataFrame(results, columns=col_names)

    def _update_row(
            self,
            row: pd.Series,
            columns: List[str],
            table: str
    ) -> None:
        """Calculate sentiment scores and add them to the database.

        :param row: A row of a Pandas DataFrame
        :param columns: The columns of text to calculate scores for
        :param table: The name of the table to insert calculated labels
        :return: None
        """
        for column in columns:
            try:
                vader_score = self._calculate_vader(row[column])
                roberta_score = self._calculate_roberta(row[column])
            except:
                logging.info(f"Could not process {row['id']}")
                continue

            query = f"""
                    UPDATE {table}
                    SET 
                        vader_score_{column} = %s,
                        roberta_score_{column} = %s
                    WHERE id = %s
                    """
            conn = self.pool.getconn()
            cur = conn.cursor()
            cur.execute(query, (vader_score, roberta_score, row["id"]))
            conn.commit()
            self.pool.putconn(conn)

    @abstractmethod
    def apply_sentiment(self):
        """Implement this method to apply sentiment values to posts stored in the database."""
        pass
