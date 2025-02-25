import logging
import traceback

import happybase
import pymongo
import trino
from hdfs import InsecureClient
from pyhive import hive
from pyspark.sql import SparkSession

from helper.config import Config

config = Config()

logger = logging.getLogger("Lakehouse")

CHUNK_SIZE = 5000


def init_connect_hbase(table_names):
    task_config = config.get_config()
    if table_names is None:
        return None, None
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
        return None, None
    tables = []
    for table_name in table_names:
        tables.append(hbase_connection.table(table_name))
    logger.info(tables)
    return tables, hbase_connection


def init_connect_mongo(node_name, collection_name):
    task_config = config.get_config()
    mongo = task_config["mongo"]
    mongo_uri = mongo["uri"]
    mongo_uri = str.replace(mongo_uri, "{NODE_NAME}", node_name)
    mongo_uri = str.replace(mongo_uri, "{NODE_REP}", str.replace(node_name, "_", ""))
    logger.info(mongo_uri)
    mongo_client = pymongo.MongoClient(mongo_uri)
    mongodb = mongo_client[node_name]
    collection = mongodb[collection_name]
    return collection, mongo_client


def init_connect_tele():
    try:
        task_config = config.get_config()
        access_token = task_config["telegram"]["access_token"]
        chat_id = task_config["telegram"]["chat_id"]
        if access_token is None or chat_id is None:
            logger.info(f"Thiếu cấu hình telegram")
        return access_token, chat_id
    except Exception as e:
        logger.error(f"Thiếu cấu hình: {str(e)}")
        traceback.print_exc()
        raise


def init_hbase_connection():
    task_config = config.get_config()
    hbase = task_config["hbase"]
    hbase_uri = hbase["uri"]
    hbase_connection = happybase.Connection(hbase_uri)
    return hbase_connection


def init_hdfs_connection():
    task_config = config.get_config()
    hdfs_info = task_config["hdfs"]
    client = InsecureClient(f'http://{hdfs_info["name_node_host"]}:{hdfs_info["port"]}', user=hdfs_info["username"],
                            timeout=600000)
    return client


def init_spark_connection():
    task_config = config.get_config()
    hdfs_info = task_config["hdfs"]
    spark = (SparkSession.builder.appName("MongoDB-to-Iceberg")
             .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.iceberg.type", "hive")
             .config("spark.sql.catalog.hive.uri", "thrift://localhost:9083")
             .config("spark.sql.catalog.iceberg.warehouse", f'hdfs://{hdfs_info["name_node_host"]}:{hdfs_info["port"]}/{hdfs_info["data_dir"]}')
             .enableHiveSupport().getOrCreate())
    return spark


def init_spark_mongodb_connection(spark, node_name, collection_name, pipeline):
    task_config = config.get_config()
    mongo = task_config["mongo"]
    mongo_uri = mongo["uri"]
    mongo_uri = str.replace(mongo_uri, "{NODE_NAME}", node_name)
    mongo_uri = str.replace(mongo_uri, "{NODE_REP}", str.replace(node_name, "_", ""))
    mongo_df_info = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", mongo_uri) \
        .option("pipeline", pipeline) \
        .option("partitioner", "MongoPaginateBySizePartitioner") \
        .option("batchSize", CHUNK_SIZE) \
        .option("database", node_name) \
        .option("collection", collection_name) \
        .load()
    return mongo_df_info


def init_hive_connection():
    task_config = config.get_config()
    hive_config = task_config["hive"]
    hive_uri = hive_config["uri"]
    hive_port = hive_config["port"]
    hive_schemas = hive_config["schema"]
    connection = hive.Connection(host=hive_uri, port=hive_port, database=hive_schemas, auth="NOSASL")
    return connection


def init_trino_connection():
    task_config = config.get_config()
    trino_config = task_config["trino"]
    trino_uri = trino_config["uri"]
    trino_port = trino_config["port"]
    trino_catalog = trino_config["catalog"]
    trino_schema = trino_config["schema"]
    connection = trino.dbapi.connect(host=trino_uri, port=trino_port, catalog=trino_catalog, schema=trino_schema)
    return connection
