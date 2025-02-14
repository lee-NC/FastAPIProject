from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Migrate to Iceberg").config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog").config("spark.sql.catalog.lakehouse.type", "hive").config("spark.sql.catalog.lakehouse.warehouse", "hdfs://localhost:9000/warehouse/").getOrCreate()

df = spark.sql("SELECT * FROM iceberg_db.cert_order")

df.write.format("iceberg").mode("append").save("lakehouse.cert_order")