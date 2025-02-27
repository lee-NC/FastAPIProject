import logging
import traceback
from datetime import datetime

import pandas as pd

from repositories.signature_transaction_repository import SignatureTransactionRepository

logger = logging.getLogger(__name__)


class SignatureTransactionService:
    def __init__(self):
        self.signature_transaction_repo = SignatureTransactionRepository()

    def get_signature_transaction(self, signature_transaction_id):
        """Gọi repository để lấy signature_transaction by id"""
        return self.signature_transaction_repo.get_by_id(signature_transaction_id)

    def count_total_signature_transaction(self, start_date: datetime, end_date: datetime, by_app: bool = False):
        """Đếm signature_transaction hoạt động"""
        try:
            result = self.signature_transaction_repo.get_total_signature_by_time(start_date, end_date, by_app)
            return result

        except Exception as e:
            logger.error(f"Error counting signature_transactions: {str(e)}")
            logger.error(traceback.format_exc())
        return []
