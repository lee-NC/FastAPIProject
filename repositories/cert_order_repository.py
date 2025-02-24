import logging
import traceback
from datetime import datetime

import pandas as pd

from helper.base_repository import BaseRepository

logger = logging.getLogger("Lakehouse")


class CertOrderRepository(BaseRepository):
    def __init__(self):
        super().__init__("cert_order")

    async def count_by_time_and_locality(self, start_date: datetime = None, end_date: datetime = None, locality: str = None):
        """Đếm số lượng credential của user theo user_ids và trạng thái"""
        try:
            end_date_str = int(end_date.timestamp() * 1000)
            query = (f"SELECT  rowkey FROM {self.table_name} "
                     f"where CAST(COALESCE(created_date, '0') AS BIGINT) <= {end_date_str} ")
            if start_date is not None:
                start_date_str = int(start_date.timestamp() * 1000)
                query = (f"SELECT  rowkey FROM {self.table_name} where CAST(COALESCE(created_date, '0') AS BIGINT) BETWEEN "
                         f"{start_date_str}  AND {end_date_str} ")
            if locality:
                query += f" AND locality_code = '{locality}'"
            res = await self.fetch_all(query, as_dataframe=True)
            return res
        except Exception:
            logger.error(traceback.format_exc())
        return 0
