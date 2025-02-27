import logging
import traceback
from datetime import datetime

import pandas as pd

from helper.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class CredentialRepository(BaseRepository):
    def __init__(self):
        super().__init__("credential")

    def count_valid_credential(self, start_date: datetime = None, end_date: datetime = None, locality: str = None):
        """Đếm số lượng credential được tạo"""
        try:
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
            query = (f"SELECT COUNT(Distinct id) FROM {self.table_name} where status = 0 "
                     f"and valid_from <=  TIMESTAMP '{end_date_str}' and valid_to >= TIMESTAMP '{end_date_str}' ")
            if start_date is not None:
                start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
                query = (f"SELECT COUNT(Distinct id) FROM {self.table_name} "
                         f"where valid_from BETWEEN TIMESTAMP '{end_date_str}' AND TIMESTAMP '{start_date_str}' ")
            if locality:
                query += f" AND locality_code = '{locality}'"
            res = self.fetch_all(query, as_dataframe=False)
            logger.info(type(res))
            return res
        except Exception:
            logger.error(traceback.format_exc())
        return 0

    def cert_by_account_type(self, start_date, end_date, locality):
        """Chứng thư số theo đối tượng"""
        try:
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

            start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")

            days = (end_date - start_date).days

            if 60 > days > 30:
                query_date = ("CAST(YEAR(c.created_date) AS VARCHAR) || '-W' || "
                              "LPAD(CAST(EXTRACT(WEEK FROM c.created_date) AS VARCHAR), 2, '0')")
            elif days >= 60:
                query_date = "date_format(c.created_date, '%Y-%m')"
            else:
                query_date = "date_format(c.created_date, '%Y-%m-%d')"

            query = (f"SELECT COUNT(DISTINCT c.id) as quantity, "
                     f"u.account_type, {query_date} as created_time, "
                     f"c.source, c.locality_code "
                     f"FROM (SELECT DISTINCT id, source, locality_code, created_date, identity_id FROM {self.table_name}) c "
                     f"JOIN user_info u ON u.id = c.identity_id "
                     f"WHERE c.created_date BETWEEN TIMESTAMP '{start_date_str}' AND TIMESTAMP '{end_date_str}' ")

            if locality:
                query += f" AND c.locality_code = '{locality}' "
            query += f" GROUP BY u.account_type, {query_date}, c.source, c.locality_code ORDER BY quantity DESC"
            res = self.fetch_all(query, as_dataframe=True)
            logger.info(type(res))
            return res
        except Exception:
            logger.error(traceback.format_exc())
        return 0
