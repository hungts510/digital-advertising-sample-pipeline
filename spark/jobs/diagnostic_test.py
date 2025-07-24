#!/usr/bin/env python3
"""
Comprehensive diagnostic script for Spark-MinIO connectivity issues
Run this from within your Spark container to identify the problem
"""

import os
import sys
from pyspark.sql import SparkSession

def test_basic_connectivity():
    """Test basic network connectivity to MinIO"""
    print("=== STEP 1: Testing Basic Connectivity ===")
    
    # Test if MinIO is reachable
    import subprocess
    try:
        result = subprocess.run(['curl', '-f', 'http://minio:9000/minio/health/live'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("✅ MinIO health check passed")
        else:
            print(f"❌ MinIO health check failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Network connectivity test failed: {e}")
        return False
    
    # Test if we can resolve the hostname
    try:
        import socket
        ip = socket.gethostbyname('minio')
        print(f"✅ MinIO hostname resolves to: {ip}")
    except Exception as e:
        print(f"❌ DNS resolution failed: {e}")
        return False
    
    return True

def test_bucket_access():
    """Test if the bucket exists and is accessible"""
    print("\n=== STEP 2: Testing Bucket Access ===")
    
    # Try to list buckets using MinIO client approach
    try:
        import subprocess
        # First check if mc (MinIO client) is available
        result = subprocess.run(['which', 'mc'], capture_output=True, text=True)
        if result.returncode != 0:
            print("⚠️  MinIO client (mc) not available, skipping bucket test")
            return True
        
        # Configure mc alias
        subprocess.run(['mc', 'alias', 'set', 'local', 'http://minio:9000', 'minioadmin', 'minioadmin'], 
                      capture_output=True, text=True)
        
        # List buckets
        result = subprocess.run(['mc', 'ls', 'local/'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ MinIO buckets accessible:")
            print(result.stdout)
            
            # Check if sample-bucket exists
            if 'sample-bucket' in result.stdout:
                print("✅ sample-bucket exists")
                
                # List contents of sample-bucket
                result = subprocess.run(['mc', 'ls', 'local/sample-bucket/'], capture_output=True, text=True)
                if result.returncode == 0:
                    print("✅ sample-bucket contents:")
                    print(result.stdout)
                    return 'sample.csv' in result.stdout
                else:
                    print(f"❌ Could not list sample-bucket contents: {result.stderr}")
                    return False
            else:
                print("❌ sample-bucket does not exist")
                return False
        else:
            print(f"❌ Could not list buckets: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Bucket access test failed: {e}")
        return False

def test_spark_s3a_basic():
    """Test basic Spark S3A connectivity"""
    print("\n=== STEP 3: Testing Spark S3A Basic Connectivity ===")
    
    try:
        spark = SparkSession.builder \
            .appName('S3AConnectivityTest') \
            .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000') \
            .config('spark.hadoop.fs.s3a.access.key', 'minioadmin') \
            .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin') \
            .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
            .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false') \
            .config('spark.hadoop.fs.s3a.attempts.maximum', '1') \
            .config('spark.hadoop.fs.s3a.connection.establish.timeout', '5000') \
            .config('spark.hadoop.fs.s3a.connection.timeout', '10000') \
            .getOrCreate()
        
        print("✅ Spark session created successfully")
        
        # Test S3A filesystem access
        try:
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark.sparkContext._jvm.java.net.URI.create('s3a://sample-bucket/'),
                hadoop_conf
            )
            print("✅ S3A filesystem initialized")
            
            # Try to list bucket contents
            try:
                files = fs.listStatus(spark.sparkContext._jvm.org.apache.hadoop.fs.Path('s3a://sample-bucket/'))
                print(f"✅ Bucket listing successful. Found {len(files)} files:")
                for file in files:
                    print(f"  - {file.getPath()}")
                    
                # Check if sample.csv exists
                csv_found = any('sample.csv' in str(file.getPath()) for file in files)
                if csv_found:
                    print("✅ sample.csv found in bucket")
                else:
                    print("❌ sample.csv NOT found in bucket")
                    
                spark.stop()
                return csv_found
                
            except Exception as e:
                print(f"❌ Bucket listing failed: {e}")
                spark.stop()
                return False
                
        except Exception as e:
            print(f"❌ S3A filesystem initialization failed: {e}")
            spark.stop()
            return False
            
    except Exception as e:
        print(f"❌ Spark session creation failed: {e}")
        return False

def test_read_csv():
    """Test reading CSV file"""
    print("\n=== STEP 4: Testing CSV Reading ===")
    
    try:
        spark = SparkSession.builder \
            .appName('CSVReadTest') \
            .getOrCreate()
        
        print("✅ Spark session created")
        
        # Try to read the CSV
        try:
            df = spark.read.option('header', 'true').csv('s3a://sample-bucket/sample.csv')
            print("✅ CSV read operation initiated")
            
            # Try to get schema (this forces Spark to actually read the file)
            try:
                schema = df.schema
                print("✅ Schema obtained:")
                for field in schema.fields:
                    print(f"  - {field.name}: {field.dataType}")
                
                # Try to count rows
                try:
                    count = df.count()
                    print(f"✅ Row count: {count}")
                    
                    # Try to show first few rows
                    print("✅ First few rows:")
                    df.show(5)
                    
                    spark.stop()
                    return True
                    
                except Exception as e:
                    print(f"❌ Row count/show failed: {e}")
                    spark.stop()
                    return False
                    
            except Exception as e:
                print(f"❌ Schema retrieval failed: {e}")
                spark.stop()
                return False
                
        except Exception as e:
            print(f"❌ CSV read failed: {e}")
            spark.stop()
            return False
            
    except Exception as e:
        print(f"❌ Spark session creation failed: {e}")
        return False

def main():
    """Run all diagnostic tests"""
    print("Starting Spark-MinIO Diagnostic Tests...")
    print("=" * 60)
    
    # Test 1: Basic connectivity
    if not test_basic_connectivity():
        print("\n❌ DIAGNOSIS: Basic connectivity to MinIO failed")
        print("SOLUTION: Check if MinIO container is running and network connectivity")
        return
    
    # Test 2: Bucket access
    bucket_ok = test_bucket_access()
    if not bucket_ok:
        print("\n❌ DIAGNOSIS: Bucket access failed or sample.csv not found")
        print("SOLUTION: Ensure sample-bucket exists and contains sample.csv file")
        return
    
    # Test 3: Spark S3A basic connectivity
    if not test_spark_s3a_basic():
        print("\n❌ DIAGNOSIS: Spark S3A connectivity failed")
        print("SOLUTION: Check Spark S3A configuration and JAR dependencies")
        return
    
    # Test 4: CSV reading
    if not test_read_csv():
        print("\n❌ DIAGNOSIS: CSV reading failed")
        print("SOLUTION: Check file format and permissions")
        return
    
    print("\n✅ ALL TESTS PASSED! Your Spark-MinIO setup is working correctly.")

if __name__ == "__main__":
    main()