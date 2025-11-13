from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator # Changed import
from airflow.utils.dates import days_ago
import datetime

# Define the PySpark job
PYSPARK_JOB = {
    "reference": {"project_id": "google.com:hadoop-cloud-dev"}, # Replace with your project ID if different
    "placement": {"cluster_name": "cluster-yantest"},
    "pyspark_job": {
        "main_python_file_uri": "gs://yinyan-dataproc-bucket-1/deploy-auto/test.py",
    },
}

with DAG(
    dag_id='dataproc_test_dag',
    start_date=days_ago(1),
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    catchup=False,
    tags=['dataproc', 'pyspark'],
) as dag:
    submit_pyspark_job = DataprocSubmitJobOperator(  # Changed Operator
        task_id='submit_test_pyspark_job',
        job=PYSPARK_JOB,  # Pass the job definition
        region='us-central1',
        project_id='google.com:hadoop-cloud-dev', # Replace with your project ID if different
    )
