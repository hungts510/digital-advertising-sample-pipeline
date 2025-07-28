"""
Business Logic Implementation for Advertising Emissions Data
Implements 7 key business features and outputs as structured datasets.

Output Structure:
- output/top-domains-by-emissions/
- output/data-quality-issues/
- output/emission-type-contributions/
- output/domain-coverage-by-format/
- output/country-daily-trends/
- output/unusual-emission-patterns/
- output/top-5-domains/
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, avg, sum as spark_sum, count, dense_rank, row_number,
    round as spark_round, desc, asc, when, isnan, isnull,
    to_json, struct, current_timestamp, lit, stddev,
    percentile_approx, min as spark_min, max as spark_max,
    date_format, collect_list, size, expr, abs as spark_abs,
    countDistinct
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import argparse

def create_spark_session():
    """Create a Spark session with necessary configurations"""
    return (SparkSession.builder
            .appName("Advertising Emissions - Business Logic")
            .getOrCreate())

def feature_1_top_domains_by_emissions(df, output_path):
    """
    Feature 1: Calculate total emissions per domain and rank top 10
    """
    print("📊 Feature 1: Calculating top domains by emissions...")
    
    top_domains = (df.groupBy("domain")
                   .agg(
                       spark_round(spark_sum("totalEmissions"), 3).alias("total_emissions"),
                       count("*").alias("ad_count"),
                       spark_round(avg("totalEmissions"), 3).alias("avg_emissions"),
                       spark_round(spark_sum("adSelectionEmissions"), 3).alias("total_ad_selection"),
                       spark_round(spark_sum("creativeDistributionEmissions"), 3).alias("total_creative"),
                       spark_round(spark_sum("mediaDistributionEmissions"), 3).alias("total_media")
                   )
                   .withColumn("rank", dense_rank().over(Window.orderBy(col("total_emissions").desc())))
                   .filter(col("rank") <= 10)
                   .orderBy("rank"))
    
    # Add metadata
    result = top_domains.withColumn("generated_at", current_timestamp())
    
    # Save as both Parquet and CSV
    result.write.mode("overwrite").parquet(f"{output_path}/top-domains-by-emissions")
    result.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}/top-domains-by-emissions-csv")
    print(f"✅ Saved top 10 domains by emissions ({result.count()} records) - Parquet & CSV")
    return result

def feature_2_data_quality_issues(df, output_path):
    """
    Feature 2: Identify data quality issues and their impact
    """
    print("📊 Feature 2: Identifying data quality issues...")
    
    # Define data quality checks
    quality_checks = []
    
    # Check 1: Null values
    null_counts = []
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            null_counts.append((column, null_count, "null_values"))
    
    # Check 2: Negative total emissions (unusual but possible)
    negative_emissions = df.filter(col("totalEmissions") < 0).count()
    if negative_emissions > 0:
        null_counts.append(("totalEmissions", negative_emissions, "negative_values"))
    
    # Check 3: Component emissions exceed total (data consistency)
    inconsistent_totals = df.filter(
        (col("adSelectionEmissions") + col("creativeDistributionEmissions") + col("mediaDistributionEmissions")) 
        > (col("totalEmissions") * 1.1)  # Allow 10% tolerance
    ).count()
    if inconsistent_totals > 0:
        null_counts.append(("emission_components", inconsistent_totals, "component_exceeds_total"))
    
    # Check 4: Extreme outliers (beyond 3 standard deviations)
    stats = df.agg(
        avg("totalEmissions").alias("mean_emissions"),
        stddev("totalEmissions").alias("stddev_emissions")
    ).collect()[0]
    
    if stats["stddev_emissions"] is not None:
        threshold = stats["mean_emissions"] + (3 * stats["stddev_emissions"])
        outliers = df.filter(col("totalEmissions") > threshold).count()
        if outliers > 0:
            null_counts.append(("totalEmissions", outliers, "extreme_outliers"))
    
    # Create DataFrame from quality issues
    if null_counts:
        quality_df = spark.createDataFrame(null_counts, ["column_name", "issue_count", "issue_type"])
        quality_df = quality_df.withColumn(
            "impact_percentage", 
            spark_round((col("issue_count") / lit(df.count())) * 100, 2)
        ).withColumn("generated_at", current_timestamp())
    else:
        # No issues found
        quality_df = spark.createDataFrame(
            [("no_issues", 0, "clean_data", 0.0)], 
            ["column_name", "issue_count", "issue_type", "impact_percentage"]
        ).withColumn("generated_at", current_timestamp())
    
    # Save as both Parquet and CSV
    quality_df.write.mode("overwrite").parquet(f"{output_path}/data-quality-issues")
    quality_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}/data-quality-issues-csv")
    print(f"✅ Saved data quality analysis ({quality_df.count()} issues found) - Parquet & CSV")
    return quality_df

def feature_3_emission_type_contributions(df, output_path):
    """
    Feature 3: Compute percentage contribution of each emission type
    """
    print("📊 Feature 3: Computing emission type contributions...")
    
    # Calculate total emissions for each type
    total_emissions = df.agg(
        spark_sum("adSelectionEmissions").alias("total_ad_selection"),
        spark_sum("creativeDistributionEmissions").alias("total_creative"),
        spark_sum("mediaDistributionEmissions").alias("total_media"),
        spark_sum("totalEmissions").alias("grand_total")
    ).collect()[0]
    
    # Create percentage contributions
    contributions_data = [
        ("adSelectionEmissions", float(total_emissions["total_ad_selection"] or 0), 
         round((total_emissions["total_ad_selection"] or 0) / (total_emissions["grand_total"] or 1) * 100, 2)),
        ("creativeDistributionEmissions", float(total_emissions["total_creative"] or 0),
         round((total_emissions["total_creative"] or 0) / (total_emissions["grand_total"] or 1) * 100, 2)),
        ("mediaDistributionEmissions", float(total_emissions["total_media"] or 0),
         round((total_emissions["total_media"] or 0) / (total_emissions["grand_total"] or 1) * 100, 2))
    ]
    
    contributions_df = spark.createDataFrame(
        contributions_data, 
        ["emission_type", "total_value", "percentage_contribution"]
    ).withColumn("generated_at", current_timestamp())
    
    # Save as both Parquet and CSV
    contributions_df.write.mode("overwrite").parquet(f"{output_path}/emission-type-contributions")
    contributions_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}/emission-type-contributions-csv")
    print(f"✅ Saved emission type contributions ({contributions_df.count()} types) - Parquet & CSV")
    return contributions_df

def feature_4_domain_coverage_by_format(df, output_path):
    """
    Feature 4: Calculate average domainCoverage per format
    """
    print("📊 Feature 4: Computing domain coverage by format...")
    
    # Convert domainCoverage to numeric if it's string
    df_numeric = df.withColumn(
        "coverage_numeric",
        when(col("domainCoverage").rlike("^[0-9.]+$"), col("domainCoverage").cast("double"))
        .otherwise(lit(None))
    )
    
    coverage_by_format = (df_numeric.groupBy("format")
                         .agg(
                             spark_round(avg("coverage_numeric"), 3).alias("avg_domain_coverage"),
                             count("*").alias("total_ads"),
                             count("coverage_numeric").alias("valid_coverage_records"),
                             spark_round(spark_min("coverage_numeric"), 3).alias("min_coverage"),
                             spark_round(spark_max("coverage_numeric"), 3).alias("max_coverage")
                         )
                         .withColumn("coverage_data_quality", 
                                   spark_round((col("valid_coverage_records") / col("total_ads")) * 100, 1))
                         .withColumn("generated_at", current_timestamp())
                         .orderBy(col("avg_domain_coverage").desc()))
    
    # Save as both Parquet and CSV
    coverage_by_format.write.mode("overwrite").parquet(f"{output_path}/domain-coverage-by-format")
    coverage_by_format.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}/domain-coverage-by-format-csv")
    print(f"✅ Saved domain coverage by format ({coverage_by_format.count()} formats) - Parquet & CSV")
    return coverage_by_format

def feature_5_country_daily_trends(df, output_path):
    """
    Feature 5: Generate country-level daily emissions trends
    """
    print("📊 Feature 5: Generating country daily trends...")
    
    daily_trends = (df.groupBy("country", "date")
                   .agg(
                       spark_round(spark_sum("totalEmissions"), 3).alias("daily_total_emissions"),
                       count("*").alias("daily_ad_count"),
                       spark_round(avg("totalEmissions"), 3).alias("daily_avg_emissions")
                   )
                   .withColumn("day_of_week", date_format("date", "EEEE"))
                   .withColumn("month_year", date_format("date", "yyyy-MM"))
                   .withColumn("generated_at", current_timestamp())
                   .orderBy("country", "date"))
    
    # Save as both Parquet and CSV
    daily_trends.write.mode("overwrite").parquet(f"{output_path}/country-daily-trends")
    daily_trends.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}/country-daily-trends-csv")
    print(f"✅ Saved country daily trends ({daily_trends.count()} records) - Parquet & CSV")
    return daily_trends

def feature_6_unusual_emission_patterns(df, output_path):
    """
    Feature 6: Flag domains with unusual emission patterns
    """
    print("📊 Feature 6: Identifying unusual emission patterns...")
    
    # Calculate domain statistics
    domain_stats = (df.groupBy("domain")
                   .agg(
                       spark_round(avg("totalEmissions"), 3).alias("avg_emissions"),
                       spark_round(stddev("totalEmissions"), 3).alias("stddev_emissions"),
                       count("*").alias("ad_count"),
                       spark_round(spark_min("totalEmissions"), 3).alias("min_emissions"),
                       spark_round(spark_max("totalEmissions"), 3).alias("max_emissions")
                   ))
    
    # Calculate overall statistics for comparison
    overall_stats = df.agg(
        avg("totalEmissions").alias("global_avg"),
        stddev("totalEmissions").alias("global_stddev")
    ).collect()[0]
    
    global_avg = overall_stats["global_avg"] or 0
    global_stddev = overall_stats["global_stddev"] or 0
    
    # Flag unusual patterns
    unusual_patterns = (domain_stats
                       .withColumn("z_score_avg", 
                                 when(lit(global_stddev) > 0, 
                                      (col("avg_emissions") - lit(global_avg)) / lit(global_stddev))
                                 .otherwise(lit(0)))
                       .withColumn("is_high_variance", 
                                 when(col("stddev_emissions") > (lit(global_stddev) * 2), lit(True))
                                 .otherwise(lit(False)))
                       .withColumn("is_outlier_avg", 
                                 when(spark_abs(col("z_score_avg")) > 2, lit(True))
                                 .otherwise(lit(False)))
                       .withColumn("pattern_flags",
                                 expr("array(" +
                                      "case when is_high_variance then 'high_variance' else null end, " +
                                      "case when is_outlier_avg then 'outlier_average' else null end" +
                                      ")"))
                       .filter((col("is_high_variance") == lit(True)) | (col("is_outlier_avg") == lit(True)))
                       .withColumn("generated_at", current_timestamp())
                       .orderBy(col("z_score_avg").desc()))
    
    # Save as both Parquet and CSV
    unusual_patterns.write.mode("overwrite").parquet(f"{output_path}/unusual-emission-patterns")
    
    # For CSV, convert array column to string
    unusual_patterns_csv = unusual_patterns.withColumn(
        "pattern_flags_str", 
        expr("array_join(filter(pattern_flags, x -> x is not null), ', ')")
    ).drop("pattern_flags").withColumnRenamed("pattern_flags_str", "pattern_flags")
    
    unusual_patterns_csv.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}/unusual-emission-patterns-csv")
    print(f"✅ Saved unusual emission patterns ({unusual_patterns.count()} domains flagged) - Parquet & CSV")
    return unusual_patterns

def feature_7_top_5_domains(df, output_path):
    """
    Feature 7: Extract top 5 domains by total emissions
    """
    print("📊 Feature 7: Extracting top 5 domains...")
    
    top_5_domains = (df.groupBy("domain")
                    .agg(
                        spark_round(spark_sum("totalEmissions"), 3).alias("total_emissions"),
                        count("*").alias("total_ads"),
                        spark_round(avg("totalEmissions"), 3).alias("avg_emissions_per_ad"),
                        countDistinct("country").alias("countries_served"),
                        countDistinct("format").alias("formats_used"),
                        countDistinct("device").alias("devices_targeted")
                    )
                    .withColumn("rank", row_number().over(Window.orderBy(col("total_emissions").desc())))
                    .filter(col("rank") <= 5)
                    .withColumn("generated_at", current_timestamp())
                    .orderBy("rank"))
    
    # Save as both Parquet and CSV
    top_5_domains.write.mode("overwrite").parquet(f"{output_path}/top-5-domains")
    top_5_domains.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}/top-5-domains-csv")
    print(f"✅ Saved top 5 domains by total emissions ({top_5_domains.count()} records) - Parquet & CSV")
    return top_5_domains

def generate_summary_report(spark, output_path, feature_results):
    """Generate a summary report of all business logic features"""
    print("📋 Generating summary report...")
    
    summary_data = [
        ("top_domains_by_emissions", feature_results[0].count(), "Top 10 domains ranked by total emissions"),
        ("data_quality_issues", feature_results[1].count(), "Data quality issues identified and quantified"),
        ("emission_type_contributions", feature_results[2].count(), "Percentage contribution by emission type"),
        ("domain_coverage_by_format", feature_results[3].count(), "Average domain coverage per advertising format"),
        ("country_daily_trends", feature_results[4].count(), "Daily emissions trends by country"),
        ("unusual_emission_patterns", feature_results[5].count(), "Domains with unusual emission patterns"),
        ("top_5_domains", feature_results[6].count(), "Top 5 domains with comprehensive metrics")
    ]
    
    summary_df = spark.createDataFrame(
        summary_data, 
        ["feature_name", "record_count", "description"]
    ).withColumn("generated_at", current_timestamp())
    
    # Save as both Parquet and CSV
    summary_df.write.mode("overwrite").parquet(f"{output_path}/business-logic-summary")
    summary_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}/business-logic-summary-csv")
    print(f"✅ Saved business logic summary - Parquet & CSV")
    return summary_df

def process_business_logic(spark, input_path, output_path):
    """
    Main function to process all business logic features
    """
    try:
        # Read transformed data
        print(f"📖 Reading transformed data from {input_path}")
        df = spark.read.parquet(input_path)
        
        total_records = df.count()
        print(f"📊 Processing {total_records} records through 7 business logic features")
        
        # Add required imports for advanced functions
        from pyspark.sql.functions import countDistinct
        
        # Execute all 7 business logic features
        feature_results = []
        
        # Feature 1: Top domains by emissions (top 10)
        result1 = feature_1_top_domains_by_emissions(df, output_path)
        feature_results.append(result1)
        
        # Feature 2: Data quality issues
        result2 = feature_2_data_quality_issues(df, output_path)
        feature_results.append(result2)
        
        # Feature 3: Emission type contributions
        result3 = feature_3_emission_type_contributions(df, output_path)
        feature_results.append(result3)
        
        # Feature 4: Domain coverage by format
        result4 = feature_4_domain_coverage_by_format(df, output_path)
        feature_results.append(result4)
        
        # Feature 5: Country daily trends
        result5 = feature_5_country_daily_trends(df, output_path)
        feature_results.append(result5)
        
        # Feature 6: Unusual emission patterns
        result6 = feature_6_unusual_emission_patterns(df, output_path)
        feature_results.append(result6)
        
        # Feature 7: Top 5 domains
        result7 = feature_7_top_5_domains(df, output_path)
        feature_results.append(result7)
        
        # Generate summary report
        summary = generate_summary_report(spark, output_path, feature_results)
        
        print("\n🎉 All business logic features completed successfully!")
        print("📁 Output datasets created (Parquet + CSV):")
        features = [
            "top-domains-by-emissions", "data-quality-issues", "emission-type-contributions",
            "domain-coverage-by-format", "country-daily-trends", "unusual-emission-patterns", 
            "top-5-domains", "business-logic-summary"
        ]
        for feature in features:
            print(f"   📄 {output_path}/{feature}/ (Parquet)")
            print(f"   📄 {output_path}/{feature}-csv/ (CSV for easy viewing)")
        
        print("\n💡 Quick CSV viewing tips:")
        print("   - CSV files are in *-csv/ directories")
        print("   - Use MinIO Console (http://localhost:9001) to download and view")
        print("   - Or copy from container: docker cp spark-master:/path/to/csv /local/path")
        
    except Exception as e:
        print(f"❌ Error during business logic processing: {str(e)}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Business Logic Implementation for Advertising Emissions")
    parser.add_argument("--input_path", required=True, help="Input transformed data path")
    parser.add_argument("--output_path", required=True, help="Output path for business logic datasets")
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Execute business logic processing
        process_business_logic(spark, args.input_path, args.output_path)
    finally:
        # Stop Spark session
        spark.stop() 