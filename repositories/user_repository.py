import logging
import traceback
from datetime import datetime

import pandas as pd

from helper.base_repository import BaseRepository

logger = logging.getLogger("Lakehouse")


class UserRepository(BaseRepository):
    def __init__(self):
        super().__init__("user_info")  # Tên bảng HBase

    async def get_active_user_ids(self, start_date: datetime = None, end_date: datetime = None, locality: str = None):
        try:
            """Đếm số lượng credential của user theo user_ids và trạng thái"""
            end_date_str = int(end_date.timestamp() * 1000)
            query = (f"SELECT  u.rowkey "
                     f"FROM {self.table_name} u join credential c on u.rowkey = c.identity_id "
                     f"where u.status = '2' and c.status = '0' and CAST(COALESCE(u.create_date, '0') AS BIGINT) <= {end_date_str} ")
            if start_date is not None:
                start_date_str = int(start_date.timestamp() * 1000)
                query = (
                    f"SELECT  u.rowkey "
                    f"FROM {self.table_name} u join credential c on u.rowkey = c.identity_id "
                    f"where u.status = '2' and c.status = '0' and CAST(COALESCE(u.create_date, '0') AS BIGINT) BETWEEN "
                    f"{start_date_str}  AND {end_date_str} ")
            if locality:
                query += f" AND u.locality_code = '{locality}'"
            res = await self.fetch_all(query, as_dataframe=True)
            if isinstance(res, pd.DataFrame):
                res = res.iloc[:, 0].tolist()
            return res
        except Exception:
            logger.error(traceback.format_exc())
        return 0
