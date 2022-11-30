from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List

from psaw import PushshiftAPI
from psycopg2.pool import ThreadedConnectionPool


class ExtractorBase(ABC):
    """A base class for all data extraction.

    Under the hood, this uses PRAW (Python Reddit API Wrapper), PSAW (Python Pushshift API
    Wrapper), and interacts with a Postgres database via psycopg2.
    """

    def __init__(self, api: PushshiftAPI, pool: ThreadedConnectionPool):
        """Initialize an ExtractorBase instance.

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

    @abstractmethod
    def _get_generator(self, *args, **kwargs):
        """Implement this method to retrieve a post generator."""
        pass

    @abstractmethod
    def _finder_process(self, *args, **kwargs):
        """Implement this method to serve as a process within the find_all method."""
        pass

    @abstractmethod
    def find_all(self, *args, **kwargs):
        """Implement this method to find all posts."""
        pass
