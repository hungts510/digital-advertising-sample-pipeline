from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum as spark_sum, count,
    round as spark_round, desc, asc,
    to_json, struct, current_timestamp
)
import argparse

def create_spark_session():
    """Create a Spark session with necessary configurations"""
    return (SparkSession.builder
            .appName("Advertising Emissions - Final Output")
            .getOrCreate())

def generate_summary_metrics(daily_metrics_df, device_format_df, country_df):
    """Generate summary metrics from transformed data"""
    
    # Overall summary
    overall_summary = daily_metrics_df.agg(
        spark_round(avg("avg_daily_emissions"), 3).alias("average_daily_emissions"),
        spark_round(spark_sum("total_daily_emissions"), 3).alias("total_emissions_all_time"),
        spark_sum("daily_ad_count").alias("total_ads_served")
    )
    
    # Top 5 device-format combinations by emissions
    top_formats = device_format_df.orderBy(desc("total_emissions")).limit(5)
    
    # Top 10 countries by emissions
    top_countries = country_df.orderBy(asc("country_rank")).limit(10)
    
    return overall_summary, top_formats, top_countries

def create_final_report(spark, input_path, output_path):
    """
    Create final analytics report:
    - Read transformed data
    - Generate summary metrics
    - Create final report
    """
    try:
        # Read transformed data
        daily_metrics = spark.read.parquet(f"{input_path}/daily_metrics")
        device_format_metrics = spark.read.parquet(f"{input_path}/device_format_metrics")
        country_rankings = spark.read.parquet(f"{input_path}/country_rankings")
        
        # Generate summary metrics
        overall_summary, top_formats, top_countries = generate_summary_metrics(
            daily_metrics, device_format_metrics, country_rankings
        )
        
        # Create final report with metadata
        final_report = overall_summary.withColumn(
            "report_metadata",
            to_json(struct(
                current_timestamp().alias("generated_at"),
                col("total_ads_served").alias("total_records"),
                lit("v1.0").alias("report_version")
            ))
        )
        
        # Save reports
        final_report.write.mode("overwrite").parquet(f"{output_path}/overall_summary")
        top_formats.write.mode("overwrite").parquet(f"{output_path}/top_formats")
        top_countries.write.mode("overwrite").parquet(f"{output_path}/top_countries")
        
        # Also save as CSV for easy viewing
        final_report.write.mode("overwrite").csv(f"{output_path}/overall_summary_csv")
        top_formats.write.mode("overwrite").csv(f"{output_path}/top_formats_csv")
        top_countries.write.mode("overwrite").csv(f"{output_path}/top_countries_csv")
        
        print("Successfully generated final reports:")
        print(f"Overall summary: {final_report.count()} record")
        print(f"Top formats: {top_formats.count()} records")
        print(f"Top countries: {top_countries.count()} records")
        
    except Exception as e:
        print(f"Error generating final report: {str(e)}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate final advertising emissions report")
    parser.add_argument("--input_path", required=True, help="Input directory with transformed data")
    parser.add_argument("--output_path", required=True, help="Output directory for final reports")
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Generate final report
        create_final_report(spark, args.input_path, args.output_path)
    finally:
        # Stop Spark session
        spark.stop() 