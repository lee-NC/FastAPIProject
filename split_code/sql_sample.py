from pyspark.sql import SparkSession;
spark = SparkSession.builder.appName("HBaseToIceberg").config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog").config("spark.sql.catalog.hadoop_prod.type", "hadoop").config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://localhost:9000/warehouse").getOrCreate();
df = spark.read.format("org.apache.hadoop.hbase.spark").option("hbase.table", "user_info").option("hbase.spark.use.hbasecontext", "false").load();
df.write.format("iceberg").mode("overwrite").save("lakehouse.user_info")
