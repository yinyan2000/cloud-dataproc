from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.utils.dates import days_ago

# Define the PySpark batch job for Dataproc Serverless
PYSPARK_BATCH = {
    "pyspark_batch": {
        "main_python_file_uri": "gs://yinyan-dataproc-bucket-1/deploy-auto/test.py",
    },
    "environment_config": {
        "execution_config": {
            "subnetwork_uri": "default-auto"
        }
    }
}

with DAG(
    dag_id='dataproc_test_dag_s8s',
    start_date=days_ago(1),
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    catchup=False,
    tags=['dataproc', 'pyspark', 'serverless'],
) as dag:
    submit_pyspark_batch = DataprocCreateBatchOperator(
        task_id='submit_pyspark_batch',
        batch=PYSPARK_BATCH,
        batch_id="test-pyspark-batch-{{ ts_nodash | lower | replace('t', '-') }}",
        region='us-central1',
        project_id='google.com:hadoop-cloud-dev',
    )
