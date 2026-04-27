from pyspark.sql import SparkSession

def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("check-counts")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse-bronze/warehouse")
        .config("spark.hadoop.fs.s3a.endpoint", "http://seaweedfs-s3:8333")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "admin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

spark = build_spark()
spark.sparkContext.setLogLevel("ERROR")
print("COUNT IN SPARK:")
spark.sql("SELECT COUNT(*) FROM lakehouse.bronze.mes_production_orders").show()
