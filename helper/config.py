import os
from pathlib import Path
import sys
from dotenv import load_dotenv
import logging

logger = logging.getLogger("Lakehouse")


class Config:
    _instance = None
    _config_data = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            # Tải cấu hình một lần tại đây
            cls._config_data = cls.get_config()
        return cls._instance

    @staticmethod
    def get_config():
        directory = Path('/usr/local/lakehouse/config')

        # Tải file config.env
        config_path = os.path.join(os.getcwd(), 'config', 'config.env')
        load_dotenv(config_path)
        task_files = [f.name for f in directory.iterdir() if f.is_file()]

        # Kiểm tra ENV và chọn file cấu hình phù hợp
        ENV = os.getenv('ENV', 'development')  # Hoặc có thể truyền tham số ENV vào nếu cần
        if ENV == 'production':
            name_files = [file for file in task_files if "production" in file]
        else:
            name_files = [file for file in task_files if "development" in file]

        if not name_files:
            print("No configuration file found!")
            sys.exit(1)

        task_file = f"config/{name_files[0]}"  # Chọn file đầu tiên từ danh sách

        import json
        with open(task_file, 'r') as f:
            config_data = json.load(f)

        return config_data

    def get_task_file(self):
        return self._config_data
