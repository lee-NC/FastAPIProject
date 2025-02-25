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
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
            query = (f"SELECT COUNT(*) "
                     f"FROM {self.table_name} u join credential c on u.id = c.identity_id "
                     f"where u.status = '2' and c.status = '0' and u.created_date <= TIMESTAMP '{end_date_str}' ")
            if start_date is not None:
                start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
                query = (
                    f"SELECT COUNT(*) FROM {self.table_name} u join credential c on u.id = c.identity_id "
                    f"where u.status = '2' and c.status = '0' "
                    f"and u.created_date BETWEEN TIMESTAMP '{start_date_str}' AND TIMESTAMP '{end_date_str}' ")
            if locality:
                query += f" AND u.locality_code = '{locality}'"
            res = await self.fetch_all(query, as_dataframe=True)
            return res
        except Exception:
            logger.error(traceback.format_exc())
        return 0
