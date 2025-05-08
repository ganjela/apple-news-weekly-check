from datetime import date, timedelta

from dotenv import load_dotenv
from google.cloud import bigquery

from utils.settings import SETTINGS

load_dotenv()


class NewsDataMonitor:
    def __init__(self):
        self.client = bigquery.Client()
        self.table_id = SETTINGS.TABLE_ID

    @staticmethod
    def _get_date_range():
        current_date = date.today()
        start = current_date - timedelta(days=6)
        return start, current_date

    def _get_published_dates(self, start, end):
        sql = self._create_query(start, end)
        rows = self.client.query(sql).result()
        found = {row.date.strftime('%Y-%m-%d') for row in rows}
        return found

    def _create_query(self, start, end):
        return f"""
            SELECT DISTINCT DATE(published_at) AS date
            FROM {self.table_id}
            WHERE DATE(published_at) BETWEEN '{start}' AND '{end}'
        """


    @staticmethod
    def _find_missing_dates(start, found):
        expected = {(start + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)}
        missing = sorted(expected - found)
        return missing

    def run(self):
        start, end = self._get_date_range()
        found = self._get_published_dates(start, end)
        missing = self._find_missing_dates(start, found)
        return missing
