import asyncio
import logging
import traceback

import pandas as pd

from helper.get_config import init_trino_connection
from functools import lru_cache

logger = logging.getLogger("Lakehouse")


class BaseRepository:
    """Base repository dùng chung cho các bảng Iceberg thông qua Trino"""

    def __init__(self, table_name: str):
        self.table_name = table_name

    @lru_cache(maxsize=10)  # Giữ cache 10 kết nối
    def get_trino_connection(self):
        return init_trino_connection()
    @staticmethod
    async def with_connection(func):
        """Hàm hỗ trợ kết nối Trino với async"""
        connection = await asyncio.to_thread(init_trino_connection)
        try:
            return await asyncio.to_thread(func, connection)
        except Exception:
            logger.error(traceback.format_exc())
        finally:
            connection.close()

    async def fetch_all(self, query: str, as_dataframe: bool = False):
        """Thực thi truy vấn SELECT và trả về kết quả."""

        async def execute_query(conn):
            async with conn.cursor() as cursor:
                await cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                rows = await cursor.fetchall()
                if not rows:
                    return 0 if "count(" in query.lower() else None if as_dataframe else []
                if "count(" in query.lower() and len(rows) == 1 and len(rows[0]) == 1:
                    return rows[0][0]
                return pd.DataFrame(rows, columns=columns) if as_dataframe else rows

        return await self.with_connection(execute_query)

    async def get_by_id(self, row_key: str):
        """Lấy dữ liệu theo ID (row key)"""
        query = f"SELECT * FROM {self.table_name} WHERE id = '{row_key}'"
        return await self.fetch_all(query, as_dataframe=True)

    async def get_all(self):
        """Lấy toàn bộ dữ liệu trong bảng"""
        query = f"SELECT * FROM {self.table_name}"
        return await self.fetch_all(query, as_dataframe=True)
