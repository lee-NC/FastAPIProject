import logging
from functools import lru_cache

from helper.get_config import init_trino_connection

logger = logging.getLogger("Lakehouse")


class BaseRepository:
    """Base repository dùng chung cho các bảng Iceberg thông qua Trino"""

    def __init__(self, table_name: str):
        self.table_name = table_name

    @lru_cache(maxsize=10)  # Giữ cache 10 kết nối
    def get_trino_connection(self):
        return init_trino_connection()

    @staticmethod
    def fetch_all(query: str, as_dataframe: bool = False):
        """Thực thi truy vấn SELECT và trả về kết quả."""
        conn = init_trino_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results if as_dataframe else results[0][0] if results else None

    def get_by_id(self, row_key: str):
        """Lấy dữ liệu theo ID (row key)"""
        query = f"SELECT * FROM {self.table_name} WHERE id = '{row_key}'"
        return self.fetch_all(query, as_dataframe=True)

    def get_all(self):
        """Lấy toàn bộ dữ liệu trong bảng"""
        query = f"SELECT * FROM {self.table_name}"
        return self.fetch_all(query, as_dataframe=True)
