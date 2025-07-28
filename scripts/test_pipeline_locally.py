from pyspark.sql import SparkSession
import sys
import os

# Add the spark/jobs directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'spark', 'jobs'))

# Import our pipeline jobs
import ingest_data
import stage_data
import transform_data
import final_output

def create_local_spark_session():
    """Create a local Spark session for testing"""
    return (SparkSession.builder
            .appName("Local Pipeline Test")
            .master("local[*]")  # Use all available cores
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            # MinIO configurations
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate())

def test_pipeline():
    """Run the complete pipeline locally"""
    print("Starting local pipeline test...")
    
    # Create Spark session
    spark = create_local_spark_session()
    
    try:
        # Test ingest_data
        print("\n1. Testing data ingestion...")
        ingest_data.ingest_data(
            spark,
            "data/raw/advertising_emissions.csv",
            "s3a://sample-bucket/raw/advertising_emissions.parquet"
        )
        
        # Test stage_data
        print("\n2. Testing data staging...")
        stage_data.stage_data(
            spark,
            "s3a://sample-bucket/raw/advertising_emissions.parquet",
            "s3a://sample-bucket/staging/advertising_emissions.parquet"
        )
        
        # Test transform_data
        print("\n3. Testing data transformation...")
        transform_data.transform_data(
            spark,
            "s3a://sample-bucket/staging/advertising_emissions.parquet",
            "s3a://sample-bucket/transformed/advertising_emissions.parquet"
        )
        
        # Test final_output
        print("\n4. Testing final output generation...")
        final_output.create_final_report(
            spark,
            "s3a://sample-bucket/transformed/advertising_emissions.parquet",
            "s3a://sample-bucket/final/advertising_emissions_analytics.parquet"
        )
        
        print("\nPipeline test completed successfully!")
        
    except Exception as e:
        print(f"\nError during pipeline test: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    test_pipeline() 