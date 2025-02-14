import logging
from datetime import datetime

from helper.base_repository import BaseRepository

logger = logging.getLogger("Lakehouse")


class UserRepository(BaseRepository):
    def __init__(self):
        super().__init__("USER_INFO")  # Tên bảng HBase


