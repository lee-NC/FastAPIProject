import logging
import traceback
from datetime import datetime

from helper.base_repository import BaseRepository
from helper.get_config import init_hbase_connection

logger = logging.getLogger("Lakehouse")


class CertOrderRepository(BaseRepository):
    def __init__(self):
        super().__init__("CERT_ORDER")

