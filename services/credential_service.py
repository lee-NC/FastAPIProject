import logging
import traceback
from datetime import datetime

from repositories.credential_repository import CredentialRepository

logger = logging.getLogger(__name__)


class CredentialService:
    def __init__(self):
        self.credential_repo = CredentialRepository()

    def get_credential(self, credential_id):
        """Gọi repository để lấy credential by id"""
        return self.credential_repo.get_by_id(credential_id)

    def create_credential(self, credential_id, credential_data):
        """Gọi repository để tạo credential"""
        return self.credential_repo.insert(credential_id, credential_data)

    def count_total_credential(self, end_date: datetime, locality: str):
        try:
            res = self.credential_repo.count_valid_credential(None, end_date, locality)
            return res
        except Exception as e:
            logger.error(f"Error count_total_credential: {str(e)}")
            logger.error(traceback.format_exc())
        return 0

    def count_new_credential(self, start_date, end_date, locality):
        try:
            return self.credential_repo.count_valid_credential(start_date, end_date, locality)
        except Exception as e:
            logger.error(f"Error count_total_credential: {str(e)}")
            logger.error(traceback.format_exc())
        return 0
