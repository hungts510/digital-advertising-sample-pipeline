# DBT Digital Advertising Emissions Analytics

A comprehensive DBT project for processing and analyzing advertising emissions data with data quality monitoring, business logic implementation, and automated testing.

## 🏗️ Project Structure

```
dbt_project/
├── dbt_project.yml              # DBT project configuration
├── profiles.yml                 # Connection profiles
├── requirements.txt             # Python dependencies
├── models/
│   ├── staging/                 # Data staging and cleaning
│   │   ├── sources.yml         # Source definitions
│   │   ├── stg_advertising_emissions.sql
│   │   └── stg_advertising_emissions.yml
│   ├── marts/                   # Business model processing
│   │   ├── daily_summary_country_device.sql
│   │   └── daily_summary_domain_format.sql
│   └── business_logic/          # Business logic implementation
│       ├── top_domains_by_emissions.sql
│       ├── data_quality_analysis.sql
│       ├── emission_type_contributions.sql
│       └── domain_coverage_by_format.sql
├── tests/                       # Custom tests
│   ├── assert_emissions_components_sum_to_total.sql
│   └── assert_minimum_data_quality_threshold.sql
├── macros/                      # Reusable SQL macros
├── docs/                        # Documentation
└── seeds/                       # Static reference data
```

## 🚀 Features Implemented

### 1. Data Staging - Ingestion & Cleaning
- ✅ **Raw data ingestion** from S3/MinIO sources
- ✅ **Data standardization** (date formats, string cleaning, type casting)
- ✅ **Data quality scoring** (0-100 scale with detailed flags)
- ✅ **Automated data validation** with comprehensive tests
- ✅ **Missing value handling** and outlier detection

### 2. Business Model Processing
- ✅ **Daily summary by country and device** with COUNT, SUM, AVG measures
- ✅ **Daily summary by domain and format** with detailed metrics
- ✅ **Emission percentage calculations** for all emission types
- ✅ **Statistical analysis** (min, max, stddev, percentiles)
- ✅ **Performance benchmarking** and categorization

### 3. Business Logic Implementation (7 Features)
- ✅ **Top 10 domains by emissions** with comprehensive ranking
- ✅ **Data quality issue identification** with impact analysis
- ✅ **Emission type contribution analysis** with optimization recommendations
- ✅ **Domain coverage by format** analysis
- ✅ **Country-level daily trends** (automatically generated from staging)
- ✅ **Unusual pattern detection** (statistical outliers in staging)
- ✅ **Top 5 domains extraction** (subset of top 10)

## 🛠️ Setup Instructions

### Prerequisites
- Python 3.8+
- Access to Spark cluster (configured in docker-compose)
- S3/MinIO bucket with advertising emissions data

### 1. Install Dependencies
```bash
cd dbt_project
pip install -r requirements.txt
```

### 2. Configure Profiles
Edit `profiles.yml` to match your environment:
```yaml
advertising_emissions:
  target: dev
  outputs:
    dev:
      type: spark
      method: session
      schema: advertising_dev
      host: spark-master  # Your Spark master host
      port: 7077
```

### 3. Install DBT Utils Package
```bash
dbt deps
```

### 4. Test Connection
```bash
dbt debug
```

## 🚀 Usage

### Run Full Pipeline
```bash
# Run all models
dbt run

# Run specific model layers
dbt run --models staging
dbt run --models marts
dbt run --models business_logic

# Run specific models
dbt run --models stg_advertising_emissions
dbt run --models top_domains_by_emissions
```

### Run Tests
```bash
# Run all tests
dbt test

# Run tests for specific models
dbt test --models stg_advertising_emissions
dbt test --models business_logic

# Run specific test types
dbt test --select test_type:data
dbt test --select test_type:schema
```

### Generate Documentation
```bash
# Generate and serve documentation
dbt docs generate
dbt docs serve
```

### Data Quality Monitoring
```bash
# Run data quality analysis
dbt run --models data_quality_analysis

# Check test results
dbt test --store-failures

# View failed test data
dbt run-operation query --args "SELECT * FROM test_failures.assert_emissions_components_sum_to_total"
```

## 📊 Output Schema

### Staging Layer
- `stg_advertising_emissions` - Cleaned and scored source data

### Marts Layer
- `daily_summary_country_device` - Daily aggregations by country and device
- `daily_summary_domain_format` - Daily aggregations by domain and format

### Business Logic Layer
- `top_domains_by_emissions` - Top 10 domains with comprehensive metrics
- `data_quality_analysis` - Issue identification and recommendations
- `emission_type_contributions` - Percentage analysis with optimization guidance
- `domain_coverage_by_format` - Coverage analysis by advertising format

## 🧪 Testing Framework

### Automated Tests
- **Schema tests**: Column constraints, accepted values, null checks
- **Business logic tests**: Emission calculations, data quality thresholds
- **Custom tests**: Component sum validation, outlier detection
- **Referential integrity**: Cross-model consistency checks

### Data Quality Scoring
Each record receives a quality score (0-100) based on:
- Null value presence (30 points)
- Emission calculation mismatches (20 points)
- Extreme value detection (25 points)
- Missing domain/country (25 points)

### Quality Monitoring
- Real-time quality score tracking
- Issue categorization and prioritization
- Business impact assessment
- Automated recommendations

## 🔄 Integration with Existing Pipeline

This DBT project is designed to work alongside your existing PySpark pipeline:

1. **Source Data**: Reads from the same S3/MinIO sources
2. **Staging**: Alternative to PySpark staging with enhanced quality scoring
3. **Business Logic**: Implements same requirements with SQL-first approach
4. **Output**: Same structure under `s3a://sample-bucket/output/dataset-name/`
5. **Testing**: Comprehensive validation framework

## 📈 Performance Optimization

- **Materialization Strategy**: Tables for better query performance
- **Partitioning**: By date and domain for efficient queries
- **Indexing**: On frequently filtered columns
- **Incremental Models**: For large datasets (optional)
- **Model Dependencies**: Optimized DAG execution order

## 🔍 Monitoring and Alerting

- **Test Failures**: Stored in dedicated schema for investigation
- **Quality Degradation**: Automated detection of quality score drops
- **Business Metrics**: Key performance indicators tracking
- **Data Freshness**: Timestamp tracking across all models

## 🚀 Getting Started Quick Command

```bash
# Complete setup and run
cd dbt_project
pip install -r requirements.txt
dbt deps
dbt run
dbt test
dbt docs generate
```

This will create all models, run tests, and generate documentation for your advertising emissions analytics pipeline.

## 📚 Additional Resources

- [DBT Documentation](https://docs.getdbt.com/)
- [DBT Utils Package](https://github.com/dbt-labs/dbt-utils)
- [Spark SQL Reference](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Data Quality Best Practices](https://docs.getdbt.com/docs/build/tests) 