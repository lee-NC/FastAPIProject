import logging
import traceback
from datetime import datetime

from repositories.credential_repository import CredentialRepository
from repositories.user_repository import UserRepository

logger = logging.getLogger(__name__)


class UserService:
    def __init__(self):
        self.user_repo = UserRepository()
        self.credential_repo = CredentialRepository()

    async def get_user(self, user_id):
        """Gọi repository để lấy user by id"""
        return await self.user_repo.get_user_by_id(user_id)

    async def create_user(self, user_id, user_data):
        """Gọi repository để tạo user"""
        return await self.user_repo.insert_user(user_id, user_data)

    async def count_total_user_active(self, end_date: datetime, locality: str):
        """Đếm user hoạt động"""
        try:
            timestamp = str(int(end_date.timestamp() * 1000))
            filter1 = "SingleColumnValueFilter('INFO', 'STATUS', =, 'binary:2')"
            filter2 = f"SingleColumnValueFilter('INFO', 'CREATE_DATE', <=, 'binary:{timestamp}')"
            filter3 = f"SingleColumnValueFilter('INFO', 'LOCALITY_CODE', =, 'binary:{locality}')"
            filters = f"({filter1}) AND ({filter2})"
            if locality is not None and locality != "":
                filters = f"({filter1}) AND ({filter2}) AND ({filter3})"
            logger.info(filters)
            user_ids = await self.user_repo.get_ids_by_query(filters)
            if len(user_ids) == 0:
                return 0
            # Đếm số user hợp lệ
            filters = "SingleColumnValueFilter('INFO', 'STATUS', =, 'binary:0')"
            logger.info(filters)
            valid_users = await self.credential_repo.count_credential_by_user_ids(filters, user_ids)
            return valid_users

        except Exception as e:
            logger.error(f"Error counting active users: {str(e)}")
            logger.error(traceback.format_exc())
        return 0

    async def count_new_user_active(self, start_date: datetime, end_date: datetime, locality: str):
        try:
            end_timestamp = str(int(end_date.timestamp() * 1000))
            start_timestamp = str(int(start_date.timestamp() * 1000))
            filter_1 = "SingleColumnValueFilter('INFO', 'STATUS', =, 'binary:2')"
            if locality is not None and locality != "":
                filter_1 += f" AND SingleColumnValueFilter('INFO', 'LOCALITY_CODE', =, 'binary:{locality}')"
            # Tạo Filter List 2 (OR)
            filter_2 = f"SingleColumnValueFilter('INFO', 'CREATE_DATE', >=, 'binary:{start_timestamp}') AND SingleColumnValueFilter('INFO', 'CREATE_DATE', <=, 'binary:{end_timestamp}')"

            # Kết hợp cả hai filter với AND
            filters = f"({filter_1}) AND ({filter_2})"
            logger.info(filters)
            new_users = await self.user_repo.count(filters)
            return new_users

        except Exception as e:
            logger.error(f"Error counting active users: {str(e)}")
            logger.error(traceback.format_exc())
        return 0
