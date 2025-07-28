from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, when, sum as spark_sum,
    round as spark_round, isnan, isnull
)
import argparse

def create_spark_session():
    """Create a Spark session with necessary configurations"""
    return (SparkSession.builder
            .appName("Advertising Emissions - Staging")
            .getOrCreate())

def validate_emissions_data(df):
    """Validate emissions data for consistency"""
    # Check if total emissions matches sum of individual emissions
    validation = df.withColumn(
        "calculated_total",
        spark_round(
            col("adSelectionEmissions") +
            col("creativeDistributionEmissions") +
            col("mediaDistributionEmissions"),
            3
        )
    ).withColumn(
        "is_total_valid",
        (col("calculated_total") - col("totalEmissions")).between(-0.001, 0.001)
    )
    
    # Count invalid totals
    invalid_count = validation.filter(~col("is_total_valid")).count()
    print(f"Found {invalid_count} records with invalid total emissions")
    
    return validation

def clean_data(df):
    """
    Clean and standardize the data:
    - Convert strings to lowercase
    - Remove leading/trailing spaces
    - Handle missing values
    - Validate emissions data
    """
    try:
        # Clean string columns
        cleaned_df = df.withColumn("domain", lower(trim(col("domain"))))
        cleaned_df = cleaned_df.withColumn("format", lower(trim(col("format"))))
        cleaned_df = cleaned_df.withColumn("country", trim(col("country")))
        cleaned_df = cleaned_df.withColumn("device", lower(trim(col("device"))))
        cleaned_df = cleaned_df.withColumn("domainCoverage", lower(trim(col("domainCoverage"))))
        
        # Handle missing values in numeric columns
        numeric_cols = ["adSelectionEmissions", "creativeDistributionEmissions", 
                       "mediaDistributionEmissions", "totalEmissions"]
        
        for col_name in numeric_cols:
            cleaned_df = cleaned_df.withColumn(
                col_name,
                when(
                    (isnan(col(col_name)) | isnull(col(col_name))),
                    0
                ).otherwise(col(col_name))
            )
        
        # Validate emissions data
        validated_df = validate_emissions_data(cleaned_df)
        
        # Remove temporary validation columns
        final_df = validated_df.drop("calculated_total", "is_total_valid")
        
        return final_df
        
    except Exception as e:
        print(f"Error during data cleaning: {str(e)}")
        raise

def stage_data(spark, input_path, output_path):
    """
    Stage the data:
    - Read Parquet
    - Clean and validate
    - Save cleaned data
    """
    try:
        # Read Parquet file
        df = spark.read.parquet(input_path)
        
        # Clean and validate
        cleaned_df = clean_data(df)
        
        # Write cleaned data
        cleaned_df.write.mode("overwrite").parquet(output_path)
        
        print(f"Successfully staged data from {input_path} to {output_path}")
        print(f"Total records processed: {cleaned_df.count()}")
        
    except Exception as e:
        print(f"Error during staging: {str(e)}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage advertising emissions data")
    parser.add_argument("--input_path", required=True, help="Input Parquet file path")
    parser.add_argument("--output_path", required=True, help="Output Parquet file path")
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Execute staging
        stage_data(spark, args.input_path, args.output_path)
    finally:
        # Stop Spark session
        spark.stop() 