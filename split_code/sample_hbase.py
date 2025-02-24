from pyspark.sql import SparkSession

# Tạo SparkSession
spark = (SparkSession.builder.appName("ReadHBase")
         .config("spark.hadoop.hbase.zookeeper.quorum", "localhost")
         .config("spark.hadoop.hbase.zookeeper.property.clientPort", "2181").getOrCreate())

# Cấu hình catalog HBase
hbase_catalog = """{
    "table":{"namespace":"default", "name":"PERSONAL_SIGN_TURN_ORDER"},
    "rowkey":"ROWKEY",
    "columns":{
        "ROWKEY":{"cf":"rowkey", "col":"ROWKEY", "type":"string"},
        "CODE":{"cf":"INFO", "col":"CODE", "type":"string"},
        "CREATED_DATE":{"cf":"INFO", "col":"CREATED_DATE", "type":"string"},
        "CREDENTIAL_ID":{"cf":"INFO", "col":"CREDENTIAL_ID", "type":"string"},
        "FULL_NAME":{"cf":"INFO", "col":"FULL_NAME", "type":"string"},
        "IDENTITY_ID":{"cf":"INFO", "col":"IDENTITY_ID", "type":"string"},
        "IS_SYNC_DHSXKD":{"cf":"INFO", "col":"IS_SYNC_DHSXKD", "type":"string"},
        "LOCALITY_CODE":{"cf":"INFO", "col":"LOCALITY_CODE", "type":"string"},
        "MA_GD":{"cf":"INFO", "col":"MA_GD", "type":"string"},
        "MA_GT":{"cf":"INFO", "col":"MA_GT", "type":"string"},
        "MA_HD":{"cf":"INFO", "col":"MA_HD", "type":"string"},
        "MA_HRM":{"cf":"INFO", "col":"MA_HRM", "type":"string"},
        "MA_KH":{"cf":"INFO", "col":"MA_KH", "type":"string"},
        "MA_TB":{"cf":"INFO", "col":"MA_TB", "type":"string"},
        "PAYMENT_ORDER_ID":{"cf":"INFO", "col":"PAYMENT_ORDER_ID", "type":"string"},
        "PAYMENT_STATUS":{"cf":"INFO", "col":"PAYMENT_STATUS", "type":"string"},
        "PAYMENT_STATUS_DESC":{"cf":"INFO", "col":"PAYMENT_STATUS_DESC", "type":"string"},
        "PRICING_CODE":{"cf":"INFO", "col":"PRICING_CODE", "type":"string"},
        "PRICING_NAME":{"cf":"INFO", "col":"PRICING_NAME", "type":"string"},
        "SIGN_TURN_NUMBER":{"cf":"INFO", "col":"SIGN_TURN_NUMBER", "type":"string"},
        "STATUS":{"cf":"INFO", "col":"STATUS", "type":"string"},
        "STATUS_DESC":{"cf":"INFO", "col":"STATUS_DESC", "type":"string"},
        "TOTAL_MONEY":{"cf":"INFO", "col":"TOTAL_MONEY", "type":"string"},
        "UID":{"cf":"INFO", "col":"UID", "type":"string"},
        "UPDATED_DATE":{"cf":"INFO", "col":"UPDATED_DATE", "type":"string"}
    }
}"""

# Đọc dữ liệu từ HBase
df = (spark.read.format("org.apache.hadoop.hbase.spark").option("catalog", hbase_catalog).load())

df.show()
