import logging
import traceback
from datetime import datetime

import pandas as pd

from helper.base_repository import BaseRepository
from model.response_dto import DashboardResponse

logger = logging.getLogger("Lakehouse")


class SignatureTransactionRepository(BaseRepository):
    def __init__(self):
        super().__init__("signature_transaction")  # Tên bảng HBase

    def get_total_signature_by_time(self, start_date: datetime = None, end_date: datetime = None,
                                          by_app: bool = False):
        """Đếm số lượng credential của user theo user_ids và trạng thái"""
        try:
            start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

            if not by_app:
                query = (
                    f"SELECT date_format(req_time, 'yyyy-MM-dd') AS day, COUNT(*) as quantity, tran_type as account_type "
                    f"FROM {self.table_name} WHERE status = 1 AND req_time BETWEEN TIMESTAMP '{start_date_str}' AND TIMESTAMP '{end_date_str}' "
                    f"GROUP BY tran_type, date_format(req_time, 'yyyy-MM-dd')")
            else:
                query = (f"SELECT app_name, COUNT(*) as quantity FROM {self.table_name} "
                         f"WHERE status = 1 AND req_time BETWEEN TIMESTAMP '{start_date_str}' AND TIMESTAMP '{end_date_str}' "
                         f"GROUP BY app_name")
            res = self.fetch_all(query, as_dataframe=True)
            return DashboardResponse(res if not res.empty else [])
        except Exception:
            logger.error(traceback.format_exc())
        return []
