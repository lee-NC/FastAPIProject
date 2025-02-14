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
            timestamp = str(int(end_date.timestamp() * 1000))
            filters = f"""SingleColumnValueFilter('INFO', 'CREATED_DATE', <=, 'binary:{timestamp}')"""

            if locality is not None and locality != "":
                filters += f" AND SingleColumnValueFilter('INFO', 'LOCALITY_CODE', =, 'binary:{locality}')"
            logger.info(filters)
            return await self.cert_order_repo.count(filters)

        except Exception as e:
            logger.error(f"Error count_total_cert_order: {str(e)}")
            logger.error(traceback.format_exc())
        return 0

    async def count_new_cert_order(self, start_date, end_date, locality):
        try:
            start_timestamp = str(int(start_date.timestamp() * 1000))
            end_timestamp = str(int(end_date.timestamp() * 1000))
            filter1 = f"SingleColumnValueFilter('INFO', 'CREATED_DATE', <=, 'binary:{end_timestamp}')"
            filter2 = f"SingleColumnValueFilter('INFO', 'CREATED_DATE', >=, 'binary:{start_timestamp}') "
            filters = f"({filter1}) AND  ({filter2})"
            if locality is not None and locality != "":
                filters += f" AND (SingleColumnValueFilter('INFO', 'LOCALITY_CODE', =, 'binary:{locality}'))"
            logger.info(filters)
            return await self.cert_order_repo.count(filters)

        except Exception as e:
            logger.error(f"Error count_total_cert_order: {str(e)}")
            logger.error(traceback.format_exc())
        return 0
