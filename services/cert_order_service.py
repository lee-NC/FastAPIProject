import logging
import traceback
from datetime import datetime

from repositories.cert_order_repository import CertOrderRepository

logger = logging.getLogger(__name__)


class CertOrderService:
    def __init__(self):
        self.cert_order_repo = CertOrderRepository()

    def get_cert_order(self, cert_order_id):
        """Gọi repository để lấy cert_order by id"""
        return self.cert_order_repo.get_by_id(cert_order_id)

    def count_total_cert_order(self, end_date: datetime, locality: str):
        """Đếm cert_order hoạt động"""
        try:
            return self.cert_order_repo.count_by_time_and_locality(None, end_date, locality)

        except Exception as e:
            logger.error(f"Error count_total_cert_order: {str(e)}")
            logger.error(traceback.format_exc())
        return 0

    def count_new_cert_order(self, start_date, end_date, locality):
        try:
            return self.cert_order_repo.count_by_time_and_locality(start_date, end_date, locality)

        except Exception as e:
            logger.error(f"Error count_new_cert_order: {str(e)}")
            logger.error(traceback.format_exc())
        return 0

    def order_by_source(self, start_date, end_date, locality):
        try:
            return self.cert_order_repo.order_by_source(start_date, end_date, locality)

        except Exception as e:
            logger.error(f"Error count_new_cert_order: {str(e)}")
            logger.error(traceback.format_exc())
        return 0

    def order_by_account_type(self, start_date, end_date, locality):
        try:
            return self.cert_order_repo.order_by_account_type(start_date, end_date, locality)

        except Exception as e:
            logger.error(f"Error count_new_cert_order: {str(e)}")
            logger.error(traceback.format_exc())
        return 0

    def order_by_status(self, start_date, end_date, locality):
        try:
            return self.cert_order_repo.order_by_status(start_date, end_date, locality)

        except Exception as e:
            logger.error(f"Error count_new_cert_order: {str(e)}")
            logger.error(traceback.format_exc())
        return 0
