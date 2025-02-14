import traceback
import logging
import asyncio
import pandas as pd
from helper.get_config import init_hive_connection

logger = logging.getLogger(__name__)


class BaseRepository:
    """Base repository dùng chung cho các bảng Hive"""

    def __init__(self, database: str, table_name: str):
        self.database = database
        self.table_name = table_name

    async def with_connection(self, func):
        """Hàm hỗ trợ kết nối Hive với async"""
        connection = await asyncio.to_thread(init_hive_connection, self.database)
        try:
            return await asyncio.to_thread(func, connection)
        except Exception:
            logger.error(traceback.format_exc())
        finally:
            connection.close()

    async def fetch_all(self, query: str, as_dataframe: bool = False):
        """Thực thi truy vấn SELECT và trả về kết quả."""

        def execute_query(conn):
            with conn.cursor() as cursor:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                if as_dataframe:
                    return pd.DataFrame(rows, columns=columns)
                return rows

        return await self.with_connection(execute_query)

    async def execute(self, query: str):
        """Thực thi các truy vấn không trả về kết quả như INSERT, UPDATE, DELETE."""
        return await self.with_connection(lambda conn: conn.cursor().execute(query))

    async def get_by_id(self, row_key: str):
        """Lấy dữ liệu theo ID (row key)"""
        query = f"SELECT * FROM {self.table_name} WHERE id = '{row_key}'"
        return await self.fetch_all(query, as_dataframe=False)

    async def get_all(self):
        """Lấy toàn bộ dữ liệu trong bảng"""
        query = f"SELECT * FROM {self.table_name}"
        return await self.fetch_all(query, as_dataframe=True)

    async def insert(self, row_key: str, data: dict):
        """Thêm dữ liệu vào bảng"""
        columns = ', '.join(data.keys())
        values = ', '.join(f"'{v}'" for v in data.values())
        query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({values})"
        return await self.execute(query)

    async def truncate(self):
        """Xóa toàn bộ dữ liệu trong bảng"""
        query = f"TRUNCATE TABLE {self.table_name}"
        return await self.execute(query)

    async def count(self, filters=None):
        """Đếm số dòng trong bảng với điều kiện filter"""
        query = f"SELECT COUNT(*) FROM {self.table_name}"
        if filters:
            query += f" WHERE {filters}"
        return await self.fetch_all(query, as_dataframe=False)

    async def get_ids_by_query(self, filters: str = ""):
        """Lấy danh sách ID theo điều kiện filter"""
        query = f"SELECT id FROM {self.table_name}"
        if filters:
            query += f" WHERE {filters}"
        return await self.fetch_all(query, as_dataframe=False)
