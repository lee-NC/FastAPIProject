import logging
import traceback
from datetime import datetime

from repositories.credential_repository import CredentialRepository

logger = logging.getLogger(__name__)


class CredentialService:
    def __init__(self):
        self.credential_repo = CredentialRepository()

    async def get_credential(self, credential_id):
        """Gọi repository để lấy credential by id"""
        return await self.credential_repo.get_by_id(credential_id)

    async def create_credential(self, credential_id, credential_data):
        """Gọi repository để tạo credential"""
        return await self.credential_repo.insert(credential_id, credential_data)

    async def count_total_credential(self, end_date: datetime, locality: str):
        try:
            res = await self.credential_repo.count_valid_credential(None, end_date, locality)
            return len(set(res))
        except Exception as e:
            logger.error(f"Error count_total_credential: {str(e)}")
            logger.error(traceback.format_exc())
        return 0

    async def count_new_credential(self, start_date, end_date, locality):
        try:
            return await self.credential_repo.count_valid_credential(start_date, end_date, locality)

        except Exception as e:
            logger.error(f"Error count_total_credential: {str(e)}")
            logger.error(traceback.format_exc())
        return 0
