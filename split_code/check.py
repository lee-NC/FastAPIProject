from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, size, element_at, year

from helper.get_config import init_connect_mongo

# Cấu hình Spark và MongoDB

spark = SparkSession.builder.appName("MongoDB-to-Iceberg").config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog").config(

    "spark.sql.catalog.iceberg.type", "hive").config("spark.sql.catalog.hive.uri", "thrift://localhost:9083").config(

    "spark.sql.catalog.iceberg.warehouse", "hdfs://localhost:9000/warehouse").enableHiveSupport().getOrCreate()

# Đọc dữ liệu từ MongoDB

collection, mongo_client = init_connect_mongo("signservice_identity", "PersonalSignTurnOrder")

# Cấu hình ngày tháng cho điều kiện tìm kiếm

start_date = datetime(2025, 1, 1)  # Ví dụ ngày bắt đầu

end_date = datetime(2025, 2, 1)  # Ví dụ ngày kết thúc

query = {

    '$and': [

        {'$or': [

            {'CreatedDate': {'$gte': start_date}},

            {'UpdatedDate': {'$gte': start_date}}

        ]},

        {'CreatedDate': {'$lt': end_date}}

    ]

}

# Lấy dữ liệu từ MongoDB với query

documents = collection.find(query).batch_size(4000)


data = list(documents)

df = spark.createDataFrame(data)

df.printSchema()

# df.show(5, truncate=False)

# Chuyển đổi các phần tử trong Pricings từ map sang struct
df = df.withColumn(
    "Pricings",
    expr("""
        transform(Pricings, x -> named_struct(
            'Name', coalesce(x['Name'], ''),
            'Code', coalesce(x['Code'], ''),
            'Price', coalesce(cast(x['Price'] as bigint), 0),
            'SignTurnNumber', coalesce(cast(x['SignTurnNumber'] as int), 0),
            'tocdo_id', coalesce(cast(x['tocdo_id'] as int), 0)
        ))
    """)
)

df_transformed = df.select(

    col("_id").cast("string").alias("id"),

    col("UserInfo._id").cast("string").alias("identity_id"),

    col("UserInfo.Uid").cast("string").alias("uid"),

    col("UserInfo.FullName").cast("string").alias("full_name"),

    col("UserInfo.LocalityCode").cast("string").alias("locality_code"),

    col("Status").cast("int").alias("status"),

    col("StatusDesc").cast("string").alias("status_desc"),

    col("CreatedDate").cast("timestamp").alias("created_date"),

    col("UpdatedDate").cast("timestamp").alias("modified_date"),

    col("DHSXKDCustomerInfo.ma_tb").cast("string").alias("ma_tb"),

    col("DHSXKDCustomerInfo.ma_gd").cast("string").alias("ma_gd"),

    col("DHSXKDCustomerInfo.ma_kh").cast("string").alias("ma_kh"),

    col("DHSXKDCustomerInfo.ma_hd").cast("string").alias("ma_hd"),

    col("DHSXKDCustomerInfo.ma_hrm").cast("string").alias("ma_hrm"),

    col("CredentialId").cast("string").alias("credential_id"),

    col("PaymentOrderId").cast("string").alias("payment_order_id"),

    col("PaymentStatus").cast("int").alias("payment_status"),

    col("PaymentStatuDesc").cast("string").alias("payment_status_desc"),

    col("IsSyncDHSXKD").cast("boolean").alias("is_sync_dhsxkd"),

    col("MaGt").cast("string").alias("ma_gt"),

    # Lấy giá trị của phần tử cuối cùng trong mảng Pricings nếu có

    when(size(col("Pricings")) > 0, element_at(col("Pricings"), -1).getField("Name"))
    .otherwise("").cast("string").alias("pricing_name"),

    when(size(col("Pricings")) > 0, element_at(col("Pricings"), -1).getField("Code"))
    .otherwise("").cast("string").alias("code"),

    when(size(col("Pricings")) > 0, element_at(col("Pricings"), -1).getField("tocdo_id"))
    .otherwise(0).cast("int").alias("pricing_code"),

    when(size(col("Pricings")) > 0, element_at(col("Pricings"), -1).getField("Price"))
    .otherwise(0).cast("bigint").alias("price"),

    when(size(col("Pricings")) > 0, element_at(col("Pricings"), -1).getField("SignTurnNumber"))
    .otherwise(0).cast("int").alias("sign_turn_number"),

    col("TotalMoney").cast("bigint").alias("total_money")

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
    INSERT (id, identity_id, uid, full_name, locality_code, status, status_desc, created_date, modified_date, ma_tb, ma_gd, ma_kh, ma_hd, ma_hrm, credential_id, payment_order_id, payment_status, payment_status_desc, is_sync_dhsxkd, ma_gt, pricing_name, code, pricing_code, price, sign_turn_number, total_money, year_created) 
    VALUES (source.id, source.identity_id, source.uid, source.full_name, source.locality_code, source.status, source.status_desc, source.created_date, source.modified_date, source.ma_tb, source.ma_gd, source.ma_kh, source.ma_hd, source.ma_hrm, source.credential_id, source.payment_order_id, source.payment_status, source.payment_status_desc, source.is_sync_dhsxkd, source.ma_gt, source.pricing_name, source.code, source.pricing_code, source.price, source.sign_turn_number, source.total_money, source.year_created);
""")
# (df_transformed.write.format("iceberg").mode("append")
#  .partitionBy("status", "year", "locality_code").option("target-file-size-bytes", 536870912)
#  .saveAsTable("iceberg.lakehouse.personal_sign_turn_order"))

print("Data has been successfully transferred from MongoDB to Iceberg!")
