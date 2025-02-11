import os
import traceback
from pathlib import Path
import sys
from dotenv import load_dotenv
import happybase
import pymongo
from hdfs import InsecureClient
import logging

logger = logging.getLogger(__name__)


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

        # Đọc dữ liệu từ file cấu hình (ví dụ ở đây giả sử file là JSON hoặc YAML)
        # Bạn có thể đọc và xử lý dữ liệu ở đây. Dưới là một ví dụ sử dụng JSON:
        import json
        with open(task_file, 'r') as f:
            config_data = json.load(f)

        return config_data

    def get_task_file(self):
        return self._config_data


def init_connect_report(config):
    try:
        task_config = config.get_config()
        hbase = task_config["hbase"]
        hbase_uri = hbase["uri"]
        hdfs_info = task_config["hdfs"]
        client = InsecureClient(f'http://{hdfs_info["name_node_host"]}:{hdfs_info["port"]}', user=hdfs_info["username"], timeout=600000)
        hbase_connection = happybase.Connection(hbase_uri)
        tables = hbase_connection.tables()  # Liệt kê các bảng
        print(f"Kết nối thành công! {tables}")
        return hbase_connection, client
    except Exception as e:
        print(f"Lỗi kết nối: {e}")
        traceback.print_exc()
        raise


def init_connect(config, node_name, collection_name, table_names):
    task_config = config.get_config()
    mongo = task_config["mongo"]
    mongo_uri = mongo["uri"]
    mongo_uri = str.replace(mongo_uri, "{NODE_NAME}", node_name)
    mongo_uri = str.replace(mongo_uri, "{NODE_REP}", str.replace(node_name, "_", ""))
    logger.info(mongo_uri)
    mongo_client = pymongo.MongoClient(mongo_uri)
    mongodb = mongo_client[node_name]
    collection = mongodb[collection_name]
    if table_names is None:
        return collection, None, None, mongo_client
    hbase = task_config["hbase"]
    hbase_uri = hbase["uri"]
    hbase_connection = happybase.Connection(hbase_uri)
    list_table = hbase_connection.tables()
    list_name = []
    for table_name in table_names:
        if table_name is None or table_name == '' or table_name not in list_table:
            continue
        else:
            list_name.append(table_name)
    if list_name is []:
        return collection, None, None, mongo_client
    tables = []
    for table_name in table_names:
        tables.append(hbase_connection.table(table_name))
    return collection, tables, hbase_connection, mongo_client


def init_connect_tele(config):
    try:
        task_config = config.get_config()
        access_token = task_config["telegram"]["access_token"]
        chat_id = task_config["telegram"]["chat_id"]
        if access_token is None or chat_id is None:
            print(f"Thiếu cấu hình telegram")
        return access_token, chat_id
    except Exception as e:
        print(f"Thiếu cấu hình: {e}")
        traceback.print_exc()
        raise
