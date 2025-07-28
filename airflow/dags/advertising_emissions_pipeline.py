from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'advertising_emissions_pipeline',
    default_args=default_args,
    description='Process advertising emissions data through multiple stages',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['advertising', 'emissions', 'data-pipeline'],
)

# Common Spark submit command
spark_submit_cmd = (
    'docker exec spark-master spark-submit '
    '--master spark://spark-master:7077 '
    '--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 '
    '--conf spark.hadoop.fs.s3a.access.key=minioadmin '
    '--conf spark.hadoop.fs.s3a.secret.key=minioadmin '
    '--conf spark.hadoop.fs.s3a.path.style.access=true '
    '--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem '
    '--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false'
)

# Task 1: Data Ingestion
ingest_data = BashOperator(
    task_id='ingest_data',
    bash_command=f'{spark_submit_cmd} /opt/bitnami/spark/jobs/ingest_data.py '
                '--input_path s3a://sample-bucket/raw/advertising_emissions.csv '
                '--output_path s3a://sample-bucket/raw/advertising_emissions.parquet',
    dag=dag
)

# Task 2: Data Staging
stage_data = BashOperator(
    task_id='stage_data',
    bash_command=f'{spark_submit_cmd} /opt/bitnami/spark/jobs/stage_data.py '
                '--input_path s3a://sample-bucket/raw/advertising_emissions.parquet '
                '--output_path s3a://sample-bucket/staging/advertising_emissions.parquet',
    dag=dag
)

# Task 3: Data Transformation
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=f'{spark_submit_cmd} /opt/bitnami/spark/jobs/transform_data.py '
                '--input_path s3a://sample-bucket/staging/advertising_emissions.parquet '
                '--output_path s3a://sample-bucket/transformed/advertising_emissions.parquet',
    dag=dag
)

# Task 4: Final Output
final_output = BashOperator(
    task_id='final_output',
    bash_command=f'{spark_submit_cmd} /opt/bitnami/spark/jobs/final_output.py '
                '--input_path s3a://sample-bucket/transformed/advertising_emissions.parquet '
                '--output_path s3a://sample-bucket/final/advertising_emissions_analytics.parquet',
    dag=dag
)

# Define task dependencies
ingest_data >> stage_data >> transform_data >> final_output 