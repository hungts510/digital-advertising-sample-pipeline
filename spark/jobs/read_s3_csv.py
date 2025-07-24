import os
from pyspark.sql import SparkSession

# MinIO/S3 connection details from environment variables (for reference, configs are in spark-defaults.conf)
BUCKET = os.environ.get('MINIO_BUCKET', 'sample-bucket')
CSV_PATH = f's3a://{BUCKET}/sample.csv'

# Create Spark session
spark = SparkSession.builder \
    .appName('ReadS3CSV') \
    .getOrCreate()

print(f'Reading CSV from {CSV_PATH}')
df = spark.read.option('header', 'true').csv(CSV_PATH)

print('Schema:')
df.printSchema()

print('First 10 rows:')
df.show(10)

spark.stop() 