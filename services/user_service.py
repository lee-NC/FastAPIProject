import logging
import traceback
from datetime import datetime

import pandas as pd

from repositories.user_repository import UserRepository

logger = logging.getLogger(__name__)


class UserService:
    def __init__(self):
        self.user_repo = UserRepository()

    def get_user(self, user_id):
        """Gọi repository để lấy user by id"""
        return self.user_repo.get_by_id(user_id)

    def create_user(self, user_id, user_data):
        """Gọi repository để tạo user"""
        return self.user_repo.insert(user_id, user_data)

    def count_total_user_active(self, end_date: datetime, locality: str):
        """Đếm user hoạt động"""
        try:
            valid_users = self.user_repo.get_active_user_ids(None, end_date, locality)
            return valid_users

        except Exception as e:
            logger.error(f"Error counting active users: {str(e)}")
            logger.error(traceback.format_exc())
        return 0

    def count_new_user_active(self, start_date: datetime, end_date: datetime, locality: str):
        try:
            valid_users = self.user_repo.get_active_user_ids(start_date, end_date, locality)
            return valid_users

        except Exception as e:
            logger.error(f"Error count_new_user_active: {str(e)}")
            logger.error(traceback.format_exc())
        return 0
