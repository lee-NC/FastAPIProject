import logging
import traceback
from datetime import datetime

from helper.base_repository import BaseRepository
from model.response_dto import DashboardResponse

logger = logging.getLogger("Lakehouse")


class SignatureTransactionRepository(BaseRepository):
    def __init__(self):
        super().__init__("signature_transaction")  # Tên bảng HBase

    def get_total_signature_by_time(self, start_date: datetime = None, end_date: datetime = None,
                                    by_app: bool = False
                                    ):
        """Đếm số lượng credential của user theo user_ids và trạng thái"""
        try:
            start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
            start_year, start_month = start_date.year, start_date.month
            end_year, end_month = end_date.year, end_date.month
            column_define = ["app_name", "quantity"]
            if by_app is True:
                column_define = ["day", "quantity", "account_type"]
                query = (
                    f"SELECT format_datetime(req_time, 'yyyy-MM-dd') AS day, COUNT(*) as quantity, trans_type_desc as account_type "
                    f"FROM {self.table_name} "
                    f"WHERE status = 1 AND req_time BETWEEN TIMESTAMP '{start_date_str}' AND TIMESTAMP '{end_date_str}' ")
                if start_year == end_year and start_month == end_month:
                    query += f"and year_created = {end_year} AND month_created = {end_month} "
                else:
                    query += (f"and ((year_created = {start_year} AND month_created >= {start_month}) "
                              f"OR (year_created > {start_year} AND year_created < {end_year}) "
                              f"OR (year_created = {end_year} AND month_created <= {end_month}))")
                query += f"GROUP BY trans_type_desc, format_datetime(req_time, 'yyyy-MM-dd')"
            else:
                query = (f"SELECT app_name, COUNT(*) as quantity FROM {self.table_name} "
                         f"WHERE status = 1 AND req_time BETWEEN TIMESTAMP '{start_date_str}' AND TIMESTAMP '{end_date_str}' ")
                if start_year == end_year and start_month == end_month:
                    query += f"and year_created = {end_year} AND month_created = {end_month} "
                else:
                    query += (f"and ((year_created = {start_year} AND month_created >= {start_month}) "
                              f"OR (year_created > {start_year} AND year_created < {end_year}) "
                              f"OR (year_created = {end_year} AND month_created <= {end_month}))")
                query += f"GROUP BY app_name ORDER BY quantity DESC LIMIT 5"
            res = self.fetch_all(query, as_dataframe=True, columns=column_define)
            return res
        except Exception:
            logger.error(traceback.format_exc())
        return []
