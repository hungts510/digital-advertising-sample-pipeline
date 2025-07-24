from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pyspark_minio_pipeline',
    default_args=default_args,
    description='PySpark pipeline: CSV from MinIO to Parquet',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

pre_step = BashOperator(
    task_id='pre_step',
    bash_command='echo "[PRE-STEP] Starting PySpark MinIO pipeline..."',
    dag=dag,
)

run_spark_job = BashOperator(
    task_id='run_pyspark_job',
    bash_command=(
        'docker exec spark-master spark-submit '
        '--master spark://spark-master:7077 '
        '--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 '
        '--conf spark.hadoop.fs.s3a.access.key=minioadmin '
        '--conf spark.hadoop.fs.s3a.secret.key=minioadmin '
        '--conf spark.hadoop.fs.s3a.path.style.access=true '
        '--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem '
        '--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false '
        '/opt/bitnami/spark/jobs/sample_job.py'
    ),
    dag=dag,
)

post_step = BashOperator(
    task_id='post_step',
    bash_command='echo "[POST-STEP] PySpark MinIO pipeline completed!"',
    dag=dag,
)

pre_step >> run_spark_job >> post_step 