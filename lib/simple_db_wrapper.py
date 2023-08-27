"""
Module implements a simple db wrapper not to bulk the main.py
"""
import atexit
import sqlite3
import logging

main_logger = logging.getLogger("main_logger")

class SimpleDb:
    """Simple db wrapper"""
    def __init__(self, db_path: str):
        """Constructor of the class"""
        self.conn = sqlite3.connect(database=db_path)
        self.cursor = self.conn.cursor()
        atexit.register(self.conn.close)

    def run_create_sql(self, sql: str):
        """
        Executes sql create statement
        """
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            main_logger.exception(
                "Failed to connect to db, it's bad: %s", e
            )
            raise e
