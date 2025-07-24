import os
from pyspark.sql import SparkSession

# MinIO/S3 connection details from environment variables
MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minioadmin')
BUCKET = os.environ.get('MINIO_BUCKET', 'sample-bucket')

# S3A paths
CSV_PATH = f's3a://{BUCKET}/sample.csv'
PARQUET_PATH = f's3a://{BUCKET}/output/'

# Create Spark session - no S3A config needed, it's all in spark-defaults.conf!
spark = SparkSession.builder \
    .appName('SampleCSVtoParquet') \
    .getOrCreate()

# # Print current configuration for debugging
# print("Current Spark S3A Configuration:")
# for item in spark.sparkContext._conf.getAll():
#     if 's3a' in item[0]:
#         print(f"  {item[0]}: {item[1]}")

# Read CSV from MinIO
print(f'Reading CSV from {CSV_PATH}')
df = spark.read.option('header', 'true').csv(CSV_PATH)
df.show()

# Simple query: filter age > 28
filtered_df = df.filter(df.age.cast('int') > 28)

# Write to Parquet in MinIO
print(f'Writing Parquet to {PARQUET_PATH}')
filtered_df.write.mode('overwrite').parquet(PARQUET_PATH)

print('✅ Job completed successfully!')
spark.stop()