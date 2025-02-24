import logging
import traceback
from datetime import datetime

from repositories.cert_order_repository import CertOrderRepository

logger = logging.getLogger(__name__)


class CertOrderService:
    def __init__(self):
        self.cert_order_repo = CertOrderRepository()

    async def get_cert_order(self, cert_order_id):
        """Gọi repository để lấy cert_order by id"""
        return await self.cert_order_repo.get_by_id(cert_order_id)

    async def create_cert_order(self, cert_order_id, cert_order_data):
        """Gọi repository để tạo cert_order"""
        return await self.cert_order_repo.insert(cert_order_id, cert_order_data)

    async def count_total_cert_order(self, end_date: datetime, locality: str):
        """Đếm cert_order hoạt động"""
        try:
            return await self.cert_order_repo.count_by_time_and_locality(None, end_date, locality)

        except Exception as e:
            logger.error(f"Error count_total_cert_order: {str(e)}")
            logger.error(traceback.format_exc())
        return 0

    async def count_new_cert_order(self, start_date, end_date, locality):
        try:
            return await self.cert_order_repo.count_by_time_and_locality(start_date, end_date, locality)

        except Exception as e:
            logger.error(f"Error count_new_cert_order: {str(e)}")
            logger.error(traceback.format_exc())
        return 0
