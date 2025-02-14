import logging
import traceback
from datetime import datetime

from repositories.credential_repository import CredentialRepository

logger = logging.getLogger(__name__)


class CredentialService:
    def __init__(self):
        self.credential_repo = CredentialRepository()

    async def count_total_credential(self, end_date: datetime, locality: str):
        try:
            timestamp = str(int(end_date.timestamp() * 1000))

            # Xây dựng filter string theo cú pháp HBase Scan
            filter1 = f"SingleColumnValueFilter('INFO', 'STATUS', =, 'binary:0')"
            filter2 = f"SingleColumnValueFilter('INFO', 'VALID_TO', >=, 'binary:{timestamp}')"
            filter3 = f"SingleColumnValueFilter('INFO', 'VALID_FROM', <=, 'binary:{timestamp}')"
            filters = f"({filter1}) AND ({filter2}) AND ({filter3})"
            if locality is not None and locality != "":
                filters += f" AND (SingleColumnValueFilter('INFO', 'LOCALITY_CODE', =, 'binary:{locality}'))"

            logger.info(filters)
            return await self.credential_repo.count(filters)

        except Exception as e:
            logger.error(f"Error count_total_credential: {str(e)}")
            logger.error(traceback.format_exc())
        return 0

    async def count_new_credential(self, start_date, end_date, locality):
        try:
            start_timestamp = str(int(start_date.timestamp() * 1000))
            end_timestamp = str(int(end_date.timestamp() * 1000))
            filter1 = f"SingleColumnValueFilter('INFO', 'VALID_FROM', >=, 'binary:{start_timestamp}')"
            filter2 = f"SingleColumnValueFilter('INFO', 'VALID_FROM', <=, 'binary:{end_timestamp}')"
            filters = f"({filter1}) AND ({filter2})"
            if locality is not None and locality != "":
                filters += f" AND (SingleColumnValueFilter('INFO', 'LOCALITY_CODE', =, 'binary:{locality}'))"
            logger.info(filters)
            return await self.credential_repo.count(filters)

        except Exception as e:
            logger.error(f"Error count_total_credential: {str(e)}")
            logger.error(traceback.format_exc())
        return 0
