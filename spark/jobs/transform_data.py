from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, avg, sum as spark_sum, count, dense_rank,
    date_format, year, month, dayofmonth,
    round as spark_round
)
import argparse

def create_spark_session():
    """Create a Spark session with necessary configurations"""
    return (SparkSession.builder
            .appName("Advertising Emissions - Transformation")
            .getOrCreate())

def calculate_metrics(df):
    """Calculate various emissions metrics"""
    
    # Daily aggregations
    daily_metrics = df.groupBy("date").agg(
        spark_round(avg("totalEmissions"), 3).alias("avg_daily_emissions"),
        spark_round(spark_sum("totalEmissions"), 3).alias("total_daily_emissions"),
        count("*").alias("daily_ad_count")
    )
    
    # Format metrics by device and format
    device_format_metrics = df.groupBy("device", "format").agg(
        spark_round(avg("totalEmissions"), 3).alias("avg_emissions"),
        spark_round(spark_sum("totalEmissions"), 3).alias("total_emissions"),
        count("*").alias("ad_count")
    )
    
    # Country rankings by total emissions
    country_window = Window.orderBy(col("total_country_emissions").desc())
    country_rankings = df.groupBy("country").agg(
        spark_round(spark_sum("totalEmissions"), 3).alias("total_country_emissions"),
        count("*").alias("country_ad_count")
    ).withColumn("country_rank", dense_rank().over(country_window))
    
    return daily_metrics, device_format_metrics, country_rankings

def transform_data(spark, input_path, output_path):
    """
    Transform the data:
    - Calculate emissions metrics
    - Create aggregations
    - Save transformed data
    """
    try:
        # Read staged data with retry logic
        print(f"Reading data from {input_path}")
        
        # First, try to read with schema inference
        try:
            df = spark.read.parquet(input_path)
        except Exception as e:
            print(f"Error reading parquet with schema inference: {e}")
            print("Trying to read with explicit schema...")
            
            # Define the schema explicitly based on our known structure
            from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
            
            schema = StructType([
                StructField("date", DateType(), True),
                StructField("domain", StringType(), True),
                StructField("format", StringType(), True),
                StructField("country", StringType(), True),
                StructField("ad_size", StringType(), True),
                StructField("device", StringType(), True),
                StructField("adSelectionEmissions", DoubleType(), True),
                StructField("creativeDistributionEmissions", DoubleType(), True),
                StructField("mediaDistributionEmissions", DoubleType(), True),
                StructField("totalEmissions", DoubleType(), True),
                StructField("domainCoverage", StringType(), True)
            ])
            
            df = spark.read.schema(schema).parquet(input_path)
        
        print(f"Successfully read {df.count()} records")
        
        # Calculate metrics
        daily_metrics, device_format_metrics, country_rankings = calculate_metrics(df)
        
        # Save transformed data
        daily_metrics.write.mode("overwrite").parquet(f"{output_path}/daily_metrics")
        device_format_metrics.write.mode("overwrite").parquet(f"{output_path}/device_format_metrics")
        country_rankings.write.mode("overwrite").parquet(f"{output_path}/country_rankings")
        
        print(f"Successfully transformed data from {input_path}")
        print(f"Daily metrics count: {daily_metrics.count()}")
        print(f"Device-format combinations: {device_format_metrics.count()}")
        print(f"Countries analyzed: {country_rankings.count()}")
        
    except Exception as e:
        print(f"Error during transformation: {str(e)}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform advertising emissions data")
    parser.add_argument("--input_path", required=True, help="Input Parquet file path")
    parser.add_argument("--output_path", required=True, help="Output directory for transformed data")
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Execute transformation
        transform_data(spark, args.input_path, args.output_path)
    finally:
        # Stop Spark session
        spark.stop() 