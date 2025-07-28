# Digital Advertising Emissions Data Pipeline

A comprehensive data engineering stack for processing digital advertising emissions data using **Apache Spark**, **DBT**, **MinIO**, **Airflow**, and **Docker Compose**. This project provides both traditional Spark-based and modern SQL-first DBT-based data pipelines for local development and prototyping.

## 🏗️ Architecture

### Tech Stack
- **Apache Spark**: Distributed data processing engine
- **DBT (Data Build Tool)**: SQL-first transformation tool
- **MinIO**: S3-compatible object storage
- **Apache Airflow**: Workflow orchestration
- **PostgreSQL**: Airflow metadata database
- **Docker & Docker Compose**: Containerization

### Data Pipeline Overview
The project implements two parallel data processing approaches:

1. **Spark Pipeline**: Traditional PySpark-based ETL pipeline
2. **DBT Pipeline**: Modern SQL-first transformation pipeline

Both pipelines process advertising emissions data through ingestion, staging, transformation, and business logic implementation stages.

## 📁 Project Structure

```
digital-advertising-sample-pipeline/
├── airflow/
│   └── dags/
│       ├── advertising_emissions_pipeline.py      # Spark-based DAG
│       └── advertising_emissions_dbt_pipeline.py  # DBT-based DAG
├── spark/
│   ├── jobs/
│   │   ├── ingest_data.py           # Data ingestion
│   │   ├── stage_data.py            # Data staging & cleaning
│   │   ├── transform_data.py        # Data transformation
│   │   ├── final_output.py          # Business logic implementation
│   │   └── test_business_model.py   # Data quality tests
│   ├── conf/                        # Spark configuration
│   └── init/                        # Initialization scripts
├── dbt_project/
│   ├── models/
│   │   ├── staging/                 # Staging models
│   │   ├── marts/                   # Data marts
│   │   └── business_logic/          # Business logic models
│   ├── tests/                       # DBT tests
│   ├── docs/                        # Documentation
│   └── Dockerfile                   # DBT container image
├── data/
│   └── raw/
│       └── advertising_emissions.csv  # Sample dataset
├── scripts/                         # Utility scripts
├── docker-compose.yml               # Service definitions
├── create-stack.sh                  # Stack setup script
└── clean-stack.sh                   # Stack cleanup script
```

## 🚀 Quick Start

### Prerequisites
- [Docker](https://www.docker.com/get-started) and [Docker Compose](https://docs.docker.com/compose/)

### 1. Clone and Setup
```bash
git clone <your-repo-url>
cd digital-advertising-sample-pipeline
```

### 2. Clean Start (Recommended)
```bash
./clean-stack.sh
```

### 3. Create and Start Services
```bash
./create-stack.sh
```

This will:
- Start all Docker services
- Initialize MinIO with sample data
- Download required Spark/Hadoop JARs
- Set up Airflow with default admin user

### 4. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` |
| **Spark Master UI** | [http://localhost:8080](http://localhost:8080) | - |
| **Airflow UI** | [http://localhost:8081](http://localhost:8081) | `admin` / `admin` |

⚠️ **Note**: Airflow may take 3-5 minutes to fully initialize after first startup.

## 📊 Data Pipelines

### Spark Pipeline (`advertising_emissions_pipeline`)
Traditional PySpark-based pipeline with the following stages:

1. **Data Ingestion**: Read CSV and convert to Parquet
2. **Data Staging**: Clean and standardize data
3. **Data Transformation**: Apply business transformations
4. **Business Logic**: Calculate metrics and aggregations
5. **Data Quality Testing**: Validate outputs

**Data Flow**:
```
s3a://sample-bucket/raw/ → s3a://sample-bucket/staging/ → s3a://sample-bucket/output/
```

### DBT Pipeline (`advertising_emissions_dbt_pipeline`)
Modern SQL-first pipeline using DBT for transformations:

1. **Data Ingestion**: Ingest to DBT-specific raw folder
2. **Data Staging**: DBT staging models
3. **DBT Build**: Run all models, tests, and documentation
4. **Documentation**: Generate DBT docs

**Data Flow**:
```
s3a://sample-bucket/dbt/raw/ → s3a://sample-bucket/dbt/staging/ → DBT Models
```

### Key Features
- **Top Domains Analysis**: Identify highest emitting domains
- **Trend Analysis**: Time-series emission patterns
- **Data Quality Checks**: Comprehensive validation rules
- **Outlier Detection**: Statistical anomaly identification
- **Emission Metrics**: Carbon footprint calculations

## 🧪 Testing & Validation

### Data Quality Checks
Both pipelines include comprehensive data quality validation:
- **Completeness**: Null value detection
- **Accuracy**: Data type and range validation
- **Consistency**: Cross-field validation
- **Timeliness**: Date range checks

### Running Tests
```bash
# Spark-based tests
docker exec spark-master spark-submit /opt/bitnami/spark/jobs/test_business_model.py

# DBT tests
docker exec dbt dbt test
```

## 🐛 Troubleshooting

### Common Issues

**Spark Jobs Failing**
- Check S3A JARs are present: `docker exec spark-master ls /opt/bitnami/spark/jars/`
- Verify MinIO connectivity: Test bucket access via MinIO console

**DBT Connection Issues**
- Ensure Spark master is running: `docker ps | grep spark-master`
- Check DBT logs: `docker logs dbt`

**Airflow DAGs Not Appearing**
- Check DAG syntax: `docker exec airflow-webserver airflow dags list`
- Review scheduler logs: `docker logs airflow-scheduler`

**Storage Issues**
- Monitor MinIO usage: Check console at [http://localhost:9001](http://localhost:9001)
- Clean old data: Use MinIO console or `mc` commands

### Log Access
```bash
# Service logs
docker logs <container-name>

# Follow logs in real-time
docker logs -f <container-name>

# Airflow task logs available in UI
```

## 🔄 Data Separation

The project maintains separate data paths for each pipeline:
- **Spark Pipeline**: `s3a://sample-bucket/{raw,staging,output}/`
- **DBT Pipeline**: `s3a://sample-bucket/dbt/{raw,staging}/`

This ensures no interference between pipeline runs and allows independent development.

## 📚 Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [DBT Documentation](https://docs.getdbt.com/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [MinIO Documentation](https://min.io/docs/)

## 🧹 Cleanup

To completely reset the environment:
```bash
./clean-stack.sh
```

This safely removes all project-related Docker resources without affecting other projects.

---

**Note**: This stack is designed for local development and prototyping. For production use, additional security, scalability, and monitoring considerations are required. 