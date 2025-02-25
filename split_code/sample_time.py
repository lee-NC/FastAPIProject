import traceback
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr, year, concat_ws, month
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, TimestampType, BooleanType, ArrayType

from helper.get_config import init_connect_mongo

# Cấu hình Spark và MongoDB
spark = SparkSession.builder.appName("MongoDB-to-Iceberg").config("spark.sql.catalog.iceberg",
                                                                  "org.apache.iceberg.spark.SparkCatalog").config(
    "spark.sql.catalog.iceberg.type", "hive").config("spark.sql.catalog.hive.uri", "thrift://localhost:9083").config(
    "spark.sql.catalog.iceberg.warehouse", "hdfs://localhost:9000/warehouse").enableHiveSupport().getOrCreate()

try:
    # Cấu hình ngày tháng cho điều kiện tìm kiếm
    current_date = datetime.now()
    end_date = datetime(current_date.year, current_date.month, current_date.day - 1, 17, 0, 0)
    start_date = end_date - timedelta(hours=1)

    collection, mongo_client = init_connect_mongo("signservice_identity", "PersonalSignTurnOrder")
    query = [
        {
            '$match': {
                '$or': [
                    {
                        '$and': [
                            {
                                'CreatedDate': {
                                    '$gte': start_date
                                }
                            }, {
                                'CreatedDate': {
                                    '$lt': end_date
                                }
                            }
                        ]
                    }, {
                        '$and': [
                            {
                                'UpdatedDate': {
                                    '$gte': start_date
                                }
                            }, {
                                'UpdatedDate': {
                                    '$lt': end_date
                                }
                            }
                        ]
                    }
                ]
            }
        }, {
            '$addFields': {
                'Pricings': {
                    '$last': '$Pricings'
                }
            }
        }, {
            '$project': {
                'identity_id': '$UserInfo._id',
                'uid': '$UserInfo.Uid',
                'full_name': '$UserInfo.FullName',
                'locality_code': '$UserInfo.LocalityCode',
                'status': '$Status',
                'status_desc': '$StatusDesc',
                'created_date': '$CreatedDate',
                'modified_date': '$UpdatedDate',
                'ma_tb': '$DHSXKDCustomerInfo.ma_tb',
                'ma_gd': '$DHSXKDCustomerInfo.ma_gd',
                'ma_kh': '$DHSXKDCustomerInfo.ma_kh',
                'ma_hd': '$DHSXKDCustomerInfo.ma_hd',
                'ma_hrm': '$DHSXKDCustomerInfo.ma_hrm',
                'credential_id': '$CredentialId',
                'payment_order_id': '$PaymentOrderId',
                'payment_status': '$PaymentStatus',
                'payment_status_desc': '$PaymentStatuDesc',
                'is_sync_dhsxkd': '$IsSyncDHSXKD',
                'ma_gt': '$MaGt',
                'pricing_name': '$Pricings.Name',
                'code': '$Pricings.Code',
                'pricing_code': '$Pricings.tocdo_id',
                'price': '$Pricings.Price',
                'sign_turn_number': '$Pricings.SignTurnNumber',
                'total_money': '$TotalMoney'
            }
        }
    ]
    documents = collection.aggregate(query).batch_size(1000)
    data = list(documents)
    df = spark.createDataFrame(data)
    df_transformed = df.select(
        col("_id").alias("id"),
        col("identity_id").alias("identity_id"),
        col("uid").alias("uid"),
        col("full_name").alias("full_name"),
        col("locality_code").alias("locality_code"),
        col("status").alias("status"),
        col("status_desc").alias("status_desc"),
        col("created_date").cast("timestamp").alias("created_date"),
        col("modified_date").cast("timestamp").alias("modified_date"),
        col("ma_tb").alias("ma_tb"),
        col("ma_gd").alias("ma_gd"),
        col("ma_kh").alias("ma_kh"),
        col("ma_hd").alias("ma_hd"),
        col("ma_hrm").alias("ma_hrm"),
        col("credential_id").alias("credential_id"),
        col("payment_order_id").alias("payment_order_id"),
        col("payment_status").cast("int").alias("payment_status"),
        col("payment_status_desc").alias("payment_status_desc"),
        col("is_sync_dhsxkd").cast("boolean").alias("is_sync_dhsxkd"),
        col("ma_gt").alias("ma_gt"),
        col("pricing_name").alias("pricing_name"),
        col("code").alias("code"),
        col("pricing_code").alias("pricing_code"),
        col("price").cast("bigint").alias("price"),
        col("sign_turn_number").alias("sign_turn_number"),
        col("total_money").cast("bigint").alias("total_money")

    )
    # Ghi vào Iceberg

    df_transformed = df_transformed.withColumn("year_created", year(col("created_date")))
    df_transformed.createOrReplaceTempView("new_data")
    spark.sql("""
            MERGE INTO iceberg.lakehouse.personal_sign_turn_order AS target
        USING (SELECT * FROM new_data) AS source
        ON target.id = source.id
        WHEN MATCHED THEN 
                UPDATE SET 
                target.status = source.status,
                target.modified_date = source.modified_date,
                target.status_desc = source.status_desc,
                target.payment_status_desc = source.payment_status_desc,
                target.payment_status = source.payment_status,
                target.is_sync_dhsxkd = source.is_sync_dhsxkd,
                target.pricing_name = source.pricing_name,
                target.code = source.code,
                target.pricing_code = source.pricing_code,
                target.price = source.price,
                target.sign_turn_number = source.sign_turn_number,
                target.total_money = source.total_money
        WHEN NOT MATCHED THEN 
            INSERT * 
            """)


except Exception as e:
    traceback.print_exc()

finally:
    if 'mongo_client' in locals() and mongo_client:
        mongo_client.close()
        print("Đóng kết nối MongoDB")
    spark.stop()
