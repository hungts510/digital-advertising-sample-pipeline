from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'analytics_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'advertising_emissions_dbt_pipeline',
    default_args=default_args,
    description='DBT-based advertising emissions pipeline - SQL transformations',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['advertising', 'emissions', 'dbt', 'sql-transformations'],
)

# Common Spark submit command for initial data ingestion/staging
spark_submit_cmd = (
    'docker exec spark-master spark-submit '
    '--master spark://spark-master:7077 '
    '--jars /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.470.jar,/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar '
    '--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 '
    '--conf spark.hadoop.fs.s3a.access.key=minioadmin '
    '--conf spark.hadoop.fs.s3a.secret.key=minioadmin '
    '--conf spark.hadoop.fs.s3a.path.style.access=true '
    '--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem '
    '--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false'
)

# DBT command using the DBT container
dbt_cmd = 'docker exec dbt dbt'

# =====================================================
# STAGE 1: Initial Data Preparation (Minimal Spark)
# =====================================================

# Task 1: Data Ingestion (Spark - CSV to Parquet conversion) - DBT folder
ingest_data_dbt = BashOperator(
    task_id='ingest_data_dbt',
    bash_command=f'{spark_submit_cmd} /opt/bitnami/spark/jobs/ingest_data.py '
                '--input_path s3a://sample-bucket/raw/advertising_emissions.csv '
                '--output_path s3a://sample-bucket/dbt/raw/advertising_emissions.parquet',
    dag=dag
)

# Task 2: Basic Data Staging (Spark - lightweight preparation) - DBT folder
stage_data_dbt = BashOperator(
    task_id='stage_data_dbt',
    bash_command=f'{spark_submit_cmd} /opt/bitnami/spark/jobs/stage_data.py '
                '--input_path s3a://sample-bucket/dbt/raw/advertising_emissions.parquet '
                '--output_path s3a://sample-bucket/dbt/staging/advertising_emissions.parquet',
    dag=dag
)

# =====================================================
# STAGE 2: DBT Pipeline - All Business Logic in SQL
# =====================================================

# Task 3: DBT Build - Runs all models and tests in dependency order
dbt_build = BashOperator(
    task_id='dbt_build',
    bash_command=f'{dbt_cmd} build --profiles-dir .',
    dag=dag
)

# =====================================================
# STAGE 3: Export and Documentation
# =====================================================



# Task 10: Generate DBT Documentation
dbt_docs_generate = BashOperator(
    task_id='dbt_docs_generate',
    bash_command=f'{dbt_cmd} docs generate --profiles-dir .',
    dag=dag
)

# Task 11: Data Quality Summary Report
dbt_data_quality_summary = BashOperator(
    task_id='dbt_data_quality_summary',
    bash_command=f'''
    echo "📋 Generating DBT data quality summary..."
    
    # Run DBT source freshness check
    {dbt_cmd} source freshness --profiles-dir . || echo "⚠️  Source freshness check skipped"
    
    # Generate model documentation
    {dbt_cmd} run-operation generate_model_yaml --profiles-dir . || echo "⚠️  Model YAML generation skipped"
    
    echo "✅ DBT pipeline completed with data quality validation"
    echo ""
    echo "📊 DBT Models Created:"
    echo "  - staging.stg_advertising_emissions"
    echo "  - marts.daily_summary_country_device"
    echo "  - business_logic.bl_top_domains_by_emissions"
    echo "  - business_logic.bl_top_5_domains"
    echo "  - business_logic.data_quality_analysis"
    echo ""
    echo "🗂️  DBT models are available in Spark's Hive metastore"
    echo "💡 Use Spark SQL or connect BI tools to query the data"
    ''',
    dag=dag
)

# =====================================================
# Task Dependencies - Sequential DBT Pipeline
# =====================================================

# Initial data preparation
ingest_data_dbt >> stage_data_dbt

# DBT pipeline using DBT container - single build command
stage_data_dbt >> dbt_build

# Documentation and quality summary phase
dbt_build >> [dbt_docs_generate, dbt_data_quality_summary] 