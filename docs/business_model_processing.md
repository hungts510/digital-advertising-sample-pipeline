# Business Model Processing Documentation

## Overview

This document describes the business model processing implementation for advertising emissions data, including aggregations, summaries, and metric calculations using PySpark.

## Requirements Coverage

### 1. Aggregations

#### Daily Summary by Country and Device
- **Purpose**: Analyze emissions patterns across geographical regions and device types
- **Grouping**: `date`, `country`, `device`
- **Metrics**: COUNT, SUM, AVG of totalEmissions and emission components

```sql
-- SQL Equivalent
SELECT 
    date,
    country,
    device,
    COUNT(*) as ad_count,
    ROUND(SUM(totalEmissions), 3) as total_emissions,
    ROUND(AVG(totalEmissions), 3) as avg_emissions,
    ROUND(SUM(adSelectionEmissions), 3) as total_ad_selection,
    ROUND(SUM(creativeDistributionEmissions), 3) as total_creative_dist,
    ROUND(SUM(mediaDistributionEmissions), 3) as total_media_dist
FROM advertising_emissions
GROUP BY date, country, device
ORDER BY date, country, device
```

#### Daily Summary by Domain and Format
- **Purpose**: Track performance across different publishers and ad formats
- **Grouping**: `date`, `domain`, `format`
- **Metrics**: COUNT, SUM, AVG of totalEmissions and emission components

```sql
-- SQL Equivalent
SELECT 
    date,
    domain,
    format,
    COUNT(*) as ad_count,
    ROUND(SUM(totalEmissions), 3) as total_emissions,
    ROUND(AVG(totalEmissions), 3) as avg_emissions,
    ROUND(SUM(adSelectionEmissions), 3) as total_ad_selection,
    ROUND(SUM(creativeDistributionEmissions), 3) as total_creative_dist,
    ROUND(SUM(mediaDistributionEmissions), 3) as total_media_dist
FROM advertising_emissions
GROUP BY date, domain, format
ORDER BY date, domain, format
```

### 2. Measures

#### Core Metrics
- **COUNT**: Number of ad impressions per group
- **SUM totalEmissions**: Total emissions for the group
- **AVG totalEmissions**: Average emissions per ad in the group

#### Emission Component Analysis
- **adSelectionEmissions**: Emissions from ad selection process
- **creativeDistributionEmissions**: Emissions from creative asset distribution
- **mediaDistributionEmissions**: Emissions from media delivery

### 3. Percentage Calculations

#### Emission Component Percentages
Calculate the percentage contribution of each emission component:

```python
# PySpark Implementation
df_with_percentages = df.withColumn(
    "adSelection_pct",
    when(col("totalEmissions") > 0, 
         (col("adSelectionEmissions") / col("totalEmissions")) * 100)
    .otherwise(0)
).withColumn(
    "creativeDistribution_pct", 
    when(col("totalEmissions") > 0,
         (col("creativeDistributionEmissions") / col("totalEmissions")) * 100)
    .otherwise(0)
).withColumn(
    "mediaDistribution_pct",
    when(col("totalEmissions") > 0,
         (col("mediaDistributionEmissions") / col("totalEmissions")) * 100)
    .otherwise(0)
)
```

## Implementation Architecture

### Data Flow
```
Raw Data → Staging → Transformation → Business Model Processing → Analytics
```

### Key Components

1. **Data Validation**: Ensures data quality and completeness
2. **Aggregation Engine**: Performs grouping and metric calculations
3. **Percentage Calculator**: Computes emission component percentages
4. **Test Suite**: Validates all business logic and calculations

### PySpark Optimizations

- **Column Pruning**: Select only required columns for processing
- **Predicate Pushdown**: Apply filters early in the pipeline
- **Caching**: Cache frequently accessed datasets
- **Partitioning**: Partition by date for optimal query performance

## Test Coverage

### Test Categories

#### 1. Aggregation Tests
- Validates correct grouping by country and device
- Validates correct grouping by domain and format
- Ensures COUNT, SUM, AVG calculations are accurate

#### 2. Data Quality Tests
- Checks for positive emission values
- Validates non-null aggregated results
- Ensures data type consistency

#### 3. Business Logic Tests
- Verifies percentage calculations sum to 100%
- Validates percentage ranges (0-100%)
- Tests mathematical accuracy of calculations

#### 4. Coverage Tests
- Ensures all countries are represented
- Validates all device types are included
- Checks format diversity in results

### Running Tests

#### Test with Real Data
```bash
# Test with pipeline data
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.470.jar,/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  /opt/bitnami/spark/jobs/test_business_model.py \
  --input_path s3a://sample-bucket/staging/advertising_emissions.parquet
```

#### Test with Generated Data
```bash
# Test with synthetic test data
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/test_business_model.py
```

## Key Business Insights

### Performance Metrics

1. **Country Analysis**
   - Total emissions by country
   - Average emissions per ad by country
   - Country ranking by efficiency

2. **Device Efficiency**
   - Desktop vs Mobile emissions comparison
   - Device-specific optimization opportunities

3. **Format Performance**
   - Banner vs Video vs Native emissions
   - Format efficiency rankings

4. **Temporal Patterns**
   - Daily emission trends
   - Seasonal variations

### Emission Component Analysis

- **Ad Selection**: Typically 30-50% of total emissions
- **Creative Distribution**: Usually 10-20% of total emissions  
- **Media Distribution**: Generally 30-60% of total emissions

## Data Quality Assurance

### Validation Rules

1. **Completeness**: All required fields present
2. **Accuracy**: Totals match sum of components
3. **Consistency**: Percentages sum to 100%
4. **Timeliness**: Recent data available for analysis

### Error Handling

- Null value handling in percentage calculations
- Division by zero protection
- Data type validation
- Range validation for percentage values

## Performance Considerations

### Optimization Strategies

1. **Data Partitioning**: Partition by date for query performance
2. **Caching**: Cache aggregated results for repeated access
3. **Column Selection**: Only load required columns
4. **Predicate Pushdown**: Apply filters at data source level

### Scalability

- Designed to handle millions of records
- Horizontal scaling with Spark cluster
- Incremental processing capabilities
- Memory-efficient aggregations

## Integration Points

### Upstream Dependencies
- Staging data from `stage_data.py`
- Clean, validated emissions data

### Downstream Consumers
- Business intelligence dashboards
- Reporting systems
- Analytics applications
- Regulatory compliance reports

## Monitoring and Alerting

### Key Metrics to Monitor
- Data freshness
- Processing time
- Data volume trends
- Calculation accuracy
- System resource usage

### Alert Conditions
- Missing daily data
- Unusual emission spikes
- Processing failures
- Data quality issues 