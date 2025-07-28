from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
import argparse

def create_spark_session():
    """Create a Spark session with necessary configurations"""
    return (SparkSession.builder
            .appName("Advertising Emissions - Ingestion")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate())

def ingest_data(spark, input_path, output_path):
    """
    Ingest CSV data and save as Parquet
    - Read CSV
    - Convert date string to date type
    - Save as Parquet
    """
    try:
        # Read CSV file
        df = spark.read.csv(
            input_path,
            header=True,
            inferSchema=True
        )
        
        # Convert date string to date type
        df = df.withColumn(
            "date",
            to_date(df.date, "MM/dd/yyyy")
        )
        
        # Write as Parquet
        df.write.mode("overwrite").parquet(output_path)
        
        print(f"Successfully ingested data from {input_path} to {output_path}")
        print(f"Total records processed: {df.count()}")
        
    except Exception as e:
        print(f"Error during ingestion: {str(e)}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest advertising emissions data")
    parser.add_argument("--input_path", required=True, help="Input CSV file path")
    parser.add_argument("--output_path", required=True, help="Output Parquet file path")
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Execute ingestion
        ingest_data(spark, args.input_path, args.output_path)
    finally:
        # Stop Spark session
        spark.stop() 