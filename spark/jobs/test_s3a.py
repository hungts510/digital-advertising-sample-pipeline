#!/usr/bin/env python3
"""
Simple test job to verify S3A connectivity with MinIO
"""

from pyspark.sql import SparkSession
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with comprehensive S3A configuration"""
    
    spark = SparkSession.builder \
        .appName("S3ATest") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.request.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.threads.max", "10") \
        .config("spark.hadoop.fs.s3a.threads.core", "5") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "10") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
        .config("spark.hadoop.fs.s3a.connection.ttl", "60000") \
        .config("spark.hadoop.fs.s3a.multipart.size", "67108864") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.block.size", "67108864") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
        .config("spark.hadoop.fs.s3a.retry.limit", "3") \
        .config("spark.hadoop.fs.s3a.executor.capacity", "48") \
        .config("spark.hadoop.fs.s3a.readahead.range", "65536") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.multipart.threshold", "67108864") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000") \
        .config("spark.hadoop.fs.s3a.list.version", "2") \
        .config("spark.hadoop.fs.s3a.directory.marker.retention", "keep") \
        .config("spark.hadoop.fs.s3a.bulk.delete.page.size", "250") \
        .config("spark.hadoop.fs.s3a.change.detection.version.required", "false") \
        .config("spark.hadoop.fs.s3a.change.detection.mode", "none") \
        .config("spark.hadoop.fs.s3a.committer.magic.enabled", "false") \
        .config("spark.hadoop.fs.s3a.committer.name", "file") \
        .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/staging") \
        .config("spark.hadoop.fs.s3a.signing-algorithm", "S3SignerType") \
        .config("spark.hadoop.fs.s3a.server-side-encryption.enabled", "false") \
        .config("spark.hadoop.fs.s3a.multipart.uploads.enabled", "true") \
        .config("spark.hadoop.fs.s3a.assumed.role.sts.endpoint.region", "us-east-1") \
        .getOrCreate()
    
    # Set additional Hadoop configuration programmatically
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "minioadmin")
    hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.multipart.purge.age", "86400000")
    hadoop_conf.set("fs.s3a.threads.keepalivetime", "60000")
    
    return spark

def main():
    """Main function to test S3A connectivity"""
    
    logger.info("Starting S3A test...")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Create test data
        logger.info("Creating test data...")
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        # Show the data
        logger.info("Test data:")
        df.show()
        
        # Try to write to S3
        logger.info("Testing S3A write...")
        df.write.mode("overwrite").csv("s3a://sample-bucket/test-output/")
        
        # Try to read from S3
        logger.info("Testing S3A read...")
        df_read = spark.read.csv("s3a://sample-bucket/test-output/")
        df_read.show()
        
        logger.info("S3A test completed successfully!")
        
    except Exception as e:
        logger.error(f"S3A test failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()