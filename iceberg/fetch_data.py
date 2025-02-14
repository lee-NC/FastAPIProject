from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Khởi tạo Spark với Iceberg
spark = SparkSession.builder.appName("MongoDB to Iceberg").config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog").config("spark.sql.catalog.lakehouse.type", "hadoop").config("spark.sql.catalog.lakehouse.warehouse", "hdfs://localhost:9000/warehouse").config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1").getOrCreate()

# Kết nối MongoDB
MONGO_URI = "mongodb://signservicecredential:SmCA2022sign@10.165.201.17:27017,10.165.201.18:27017,10.165.201.19:27017,10.163.150.69:27017/signservice_credential?replicaSet=smca&authSource=signservice_credential&readPreference=nearest&w=majority&connect=replicaSet"
MONGO_DB = "signservice_credential"
MONGO_COLLECTION_INFO = "Cert"
MONGO_COLLECTION_REQUEST = "RequestCert"

# Đọc dữ liệu từ cf_info
mongo_df_info = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", MONGO_URI).option("database", MONGO_DB).option("collection", MONGO_COLLECTION_INFO).load()

# Đọc dữ liệu từ cf_request
mongo_df_request = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", MONGO_URI).option("database", MONGO_DB).option("collection", MONGO_COLLECTION_REQUEST).load()

# Xử lý localityCode từ username
keyword = "OPER"
mongo_df_request = mongo_df_request.withColumn(
    "localityCode",
    when(col("username").isNotNull() & col("username").contains(keyword), col("username").substr(col("username").rlike(keyword), 3))
    .otherwise(col("username").substr(3, 3))
)

# Chỉ giữ request_xxx từ cf_request, đổi tên cột
filtered_request_df = mongo_df_request.select(
    col("_id").alias("id"),
    col("credentialId").alias("request_credential_id"),
    col("status").alias("request_status"),
    col("statusDesc").alias("request_status_desc"),
    col("createdByClientId").alias("request_client_id"),
    col("createdByClientName").alias("request_client_name"),
    col("updatedTime").alias("request_updated_time"),
    col("createdDate").alias("request_created_date"),
    col("pricingCode").alias("request_pricing_code"),
    col("pricingName").alias("request_pricing_name"),
    col("username").alias("request_username"),
    col("localityCode").alias("request_locality_code"),
    col("uid").alias("request_uid"),
    col("identityId").alias("request_identity_id"),
    col("fullName").alias("request_full_name"),
    col("period").alias("request_period"),
    col("code").alias("request_code"),
    col("approveTime").alias("request_approve_time"),
    col("requestType").alias("request_request_type"),
    col("requestTypeDesc").alias("request_request_type_desc")
)

# Chỉ giữ cf_info, đổi tên cột
filtered_info_df = mongo_df_info.select(
    col("requestId").alias("id"),
    col("serial").alias("serial"),
    col("status").alias("cert_status"),
    col("statusDesc").alias("cert_status_desc"),
    col("subject").alias("subject"),
    col("validFrom").alias("valid_from"),
    col("validTo").alias("valid_to"),
    col("createDate").alias("cert_created_date")
)

# Ghi đè dữ liệu cf_info vào Iceberg
filtered_info_df.write.format("iceberg").mode("overwrite").save("lakehouse.cert")

# Merge cf_request vào Iceberg
filtered_request_df.createOrReplaceTempView("request_data")
spark.sql("""
    MERGE INTO lakehouse.cert AS target
    USING request_data AS source
    ON target.id = source.id
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")
