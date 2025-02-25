import traceback
from datetime import datetime, timedelta

import pytz
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, concat_ws, year, coalesce, month
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType, ArrayType, DoubleType

from helper.get_config import init_connect_mongo

# Cấu hình Spark và MongoDB
spark = SparkSession.builder.appName("MongoDB-to-Iceberg").config("spark.sql.catalog.iceberg",
                                                                  "org.apache.iceberg.spark.SparkCatalog").config(
    "spark.sql.catalog.iceberg.type", "hive").config("spark.sql.catalog.hive.uri", "thrift://localhost:9083").config(
    "spark.sql.catalog.iceberg.warehouse", "hdfs://localhost:9000/warehouse").enableHiveSupport().getOrCreate()

try:
    # Đọc dữ liệu từ MongoDB
    collection, mongo_client = init_connect_mongo("signservice_credential", "SignatureTransaction")

    # Cấu hình ngày tháng cho điều kiện tìm kiếm
    current_date = datetime.now()
    end_date = datetime(current_date.year, current_date.month, current_date.day - 1, 17, 0, 0, tzinfo=pytz.utc)
    start_date = end_date - timedelta(minutes=20)

    query = [
        {
            '$match': {
                '$and': [
                    {
                        'finishDate': {
                            '$gte': start_date
                        }
                    }, {
                        'finishDate': {
                            '$lt': end_date
                        }
                    }
                ]
            }
        }, {
            '$project': {
                'credentialId': '$credentialId',
                'serial': '$certSerial',
                'identityId': '$identityId',
                'uid': '$identityUid',
                'fullName': '$identityName',
                'email': '$identityEmail',
                'status': '$status',
                'statusDesc': '$statusDesc',
                'reqTime': '$reqTime',
                'expiredTime': '$expireTime',
                'finishDate': '$finishDate',
                'tranTypeDesc': '$tranTypeDesc',
                'tranType': '$tranType',
                'executeTime': '$excuteTime',
                'appId': '$appId',
                'appName': '$appName',
                'tranCode': '$tranCode'
            }
        }
    ]
    print(query)
    documents = collection.aggregate(query).batch_size(1000)
    data = list(documents)

    if not data:
        print("Không có dữ liệu nào được đọc từ MongoDB.")
    else:
        print(len(data))
        df = spark.createDataFrame(data)
        df.printSchema()
        # Select và cast các cột
        df_transformed = df.select(
            col("_id").alias("id"),
            col("serial").alias("serial"),
            col("credentialId").alias("credential_id"),
            col("identityId").alias("identity_id"),
            col("uid").alias("uid"),
            col("fullName").alias("full_name"),
            col("email").alias("email"),
            col("status").cast(IntegerType()).alias("status"),
            col("statusDesc").alias("status_desc"),
            col("reqTime").cast("timestamp").alias("req_time"),
            col("finishDate").cast("timestamp").alias("finish_date"),
            col("tranTypeDesc").alias("trans_type_desc"),
            col("tranType").cast(IntegerType()).alias("trans_type"),
            col("executeTime").alias("execute_time"),
            col("appId").alias("app_id"),
            col("appName").alias("app_name"),
            col("tranCode").alias("trans_code")
        )
        df_transformed = (df_transformed.withColumn("year_created", year(col("req_time")))
                          .withColumn("month_created", year(col("req_time"))))
        table_names = "iceberg.lakehouse.signature_transaction"
        df_transformed.createOrReplaceTempView("new_data")
        (df_transformed.write.format("iceberg")
         .mode("append")
         .partitionBy('status', 'app_id', 'year_created', 'trans_type', 'month_created')
         .option("target-file-size-bytes", 536870912)
         .saveAsTable(table_names))


except Exception as e:
    traceback.print_exc()

finally:
    if 'mongo_client' in locals() and mongo_client:
        mongo_client.close()
        print("Đóng kết nối MongoDB")
    spark.stop()
