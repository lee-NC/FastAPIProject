import logging
import traceback
from datetime import datetime

import pandas as pd

from helper.base_repository import BaseRepository

logger = logging.getLogger("Lakehouse")


class CertOrderRepository(BaseRepository):
    def __init__(self):
        super().__init__("cert_order")

    async def count_by_time_and_locality(self, start_date: datetime = None, end_date: datetime = None,
                                         locality: str = None):
        """Đếm số lượng credential của user theo user_ids và trạng thái"""
        try:
            end_year, end_month = end_date.year, end_date.month
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

            query = (f"SELECT COUNT(*) FROM {self.table_name} where created_date <= TIMESTAMP '{end_date_str}' "
                     f"and year_created <= {end_year} and month_created <= {end_month}")
            if start_date is not None:
                start_year, start_month = start_date.year, start_date.month
                start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
                query = (f"SELECT COUNT(*) FROM {self.table_name} "
                         f"where created_date BETWEEN TIMESTAMP '{start_date_str}' AND TIMESTAMP '{end_date_str}' ")
                if start_year == end_year and start_month == end_month:
                    query.append(
                        f"and year_created = {end_year} AND month_created = {end_month}")
                else:
                    query.append(f"and ((year_created = {start_year} AND month_created >= {start_month}) "
                                 f"OR (year_created > {start_year} AND year_created < {end_year}) "
                                 f"OR (year_created = {end_year} AND month_created <= {end_month}))")
            if locality:
                query += f" AND locality_code = '{locality}'"
            res = await self.fetch_all(query, as_dataframe=False)
            return res
        except Exception:
            logger.error(traceback.format_exc())
        return 0
