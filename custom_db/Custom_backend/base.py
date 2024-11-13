from django.db.backends.postgresql.base import (
    DatabaseWrapper as PostgresDatabaseWrapper,
    DatabaseFeatures as PostgresFeatures
)
import psycopg2
import time
import logging

logger = logging.getLogger(__name__)




class RetryingCursor:
    def __init__(self, cursor, db_wrapper):
        self._cursor = cursor
        self._db_wrapper = db_wrapper
        self.max_retries = db_wrapper.max_retries
        self.retry_delay = db_wrapper.retry_delay

    def __getattr__(self, attr):
        return getattr(self._cursor, attr)

    def execute(self, sql, params=None):
        retries = 0
        print(f"here again coming to query ============ : 1 ")
        while True:
            try:
                return self._cursor.execute(sql, params)
            except psycopg2.OperationalError as e:
                if 'canceling statement due to conflict with recovery' in str(e):
                    if retries < self.max_retries:
                        retries += 1
                        logger.warning(
                            f"Recovery conflict detected, attempt {retries} of {self.max_retries}. "
                            f"Retrying in {self.retry_delay} seconds..."
                        )
                        time.sleep(self.retry_delay)
                        continue
                    else:
                        logger.error("Max retries reached for recovery conflict.")
                raise

    def executemany(self, sql, param_list):
        retries = 0
        print(f"here again coming to query ============ : 2 ")
        while True:
            try:
                return self._cursor.executemany(sql, param_list)
            except psycopg2.OperationalError as e:
                if 'canceling statement due to conflict with recovery' in str(e):
                    if retries < self.max_retries:
                        retries += 1
                        logger.warning(
                            f"Recovery conflict detected, attempt {retries} of {self.max_retries}. "
                            f"Retrying in {self.retry_delay} seconds..."
                        )
                        time.sleep(self.retry_delay)
                        continue
                    else:
                        logger.error("Max retries reached for recovery conflict.")
                raise


class DatabaseWrapper(PostgresDatabaseWrapper):
    def __init__(self, *args, **kwargs):
        print("here coming ...........................")
        super().__init__(*args, **kwargs)
        self.features = PostgresFeatures(self)
        self.max_retries = 3  # Configure max retry attempts
        self.retry_delay = 0.1  # Delay between retries in seconds

    def create_cursor(self, name=None):
        cursor = super().create_cursor(name)
        return RetryingCursor(cursor, self)

    def get_new_connection(self, conn_params):
        # Add extra connection parameters if needed
        conn = super().get_new_connection(conn_params)
        return conn
