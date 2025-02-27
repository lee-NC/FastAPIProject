import logging
import traceback
from datetime import datetime

import pandas as pd

from helper.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class CredentialRepository(BaseRepository):
    def __init__(self):
        super().__init__("credential")

    def count_valid_credential(self, start_date: datetime = None, end_date: datetime = None,
                                     locality: str = None):
        """Đếm số lượng credential được tạo"""
        try:
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
            query = (f"SELECT COUNT(*) FROM {self.table_name} where status = 0 "
                     f"and valid_from <=  TIMESTAMP '{end_date_str}' and valid_to >= TIMESTAMP '{end_date_str}' ")
            if start_date is not None:
                start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
                query = (f"SELECT COUNT(*) FROM {self.table_name} "
                         f"where valid_from BETWEEN TIMESTAMP '{end_date_str}' AND TIMESTAMP '{start_date_str}' ")
            if locality:
                query += f" AND locality_code = '{locality}'"
            res = self.fetch_all(query, as_dataframe=False)
            logger.info(type(res))
            return res
        except Exception:
            logger.error(traceback.format_exc())
        return 0
