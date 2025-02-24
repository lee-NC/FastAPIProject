import logging
import traceback
from datetime import datetime

import pandas as pd

from helper.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class CredentialRepository(BaseRepository):
    def __init__(self):
        super().__init__("credential")

    async def count_valid_credential(self, start_date: datetime = None, end_date: datetime = None,
                                     locality: str = None):
        """Đếm số lượng credential được tạo"""
        try:
            end_date_str = int(end_date.timestamp() * 1000)
            query = (f"SELECT rowkey FROM {self.table_name} where status = '0' "
                     f"and CAST(COALESCE(valid_from, '0') AS BIGINT) <=  {end_date_str}  "
                     f"and CAST(COALESCE(valid_to, '0') AS BIGINT) >= {end_date_str} ")
            if start_date is not None:
                start_date_str = int(start_date.timestamp() * 1000)
                query = (f"SELECT rowkey FROM {self.table_name} "
                         f"where CAST(COALESCE(valid_from, '0') AS BIGINT) <= {end_date_str}  "
                         f"and CAST(COALESCE(valid_from, '0') AS BIGINT) >= {start_date_str} ")
            if locality:
                query += f" AND locality_code = '{locality}'"
            res = await self.fetch_all(query, as_dataframe=True)
            if isinstance(res, pd.DataFrame):
                res = res.iloc[:, 0].tolist()
            return res
        except Exception:
            logger.error(traceback.format_exc())
        return 0
