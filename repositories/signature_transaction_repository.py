import logging
import traceback
from datetime import datetime

import pandas as pd

from helper.base_repository import BaseRepository
from model.data_model import DashboardResponse

logger = logging.getLogger("Lakehouse")


class SignatureTransactionRepository(BaseRepository):
    def __init__(self):
        super().__init__("signature_transaction")  # Tên bảng HBase

    async def get_total_signature_by_time(self, start_date: datetime = None, end_date: datetime = None,
                                          by_app: bool = False):
        """Đếm số lượng credential của user theo user_ids và trạng thái"""
        try:
            start_date_str = int(start_date.timestamp() * 1000)
            end_date_str = int(end_date.timestamp() * 1000)

            if not by_app:
                query = (
                    f"SELECT date_format(req_time, 'yyyy-MM-dd') AS day, COUNT(*) as quantity, tran_type as account_type "
                    f"FROM {self.table_name} WHERE status = '1' AND CAST(COALESCE(req_time, '0') AS BIGINT) "
                    f"BETWEEN {start_date_str}  AND {end_date_str}  "
                    f"GROUP BY tran_type, date_format(req_time, 'yyyy-MM-dd')")
            else:
                query = (f"SELECT app_name, COUNT(*) as quantity FROM {self.table_name} "
                         f"WHERE status = '1' AND CAST(COALESCE(req_time, '0') AS BIGINT) "
                         f"BETWEEN {start_date_str}  AND {end_date_str}  GROUP BY app_name")
            res = await self.fetch_all(query, as_dataframe=True)
            if isinstance(res, pd.DataFrame):
                res = res.iloc[:, 0].tolist()
            return DashboardResponse(res if res else [])
        except Exception:
            logger.error(traceback.format_exc())
        return []
