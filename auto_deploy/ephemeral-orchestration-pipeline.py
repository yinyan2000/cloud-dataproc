from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.dates import days_ago
import datetime

PROJECT_ID = "google.com:hadoop-cloud-dev"  # Replace with your project ID if different
REGION = "us-central1"
# Ephemeral Cluster Configuration
CLUSTER_NAME = "ephemeral-cluster-{{ dag_run.id }}"  # Unique name per DAG run
CLUSTER_CONFIG = {
    "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-4"},
    "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-4"},
    "gce_cluster_config": {"internal_ip_only": False},
}

# PySpark Job Definition
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": "gs://yinyan-dataproc-bucket-1/deploy-auto/test.py",
    },
}

with DAG(
    dag_id='dataproc_test_dag_ephemeral',  # Changed DAG ID to avoid confusion
    start_date=days_ago(1),
    schedule_interval='*/5 * * * *',  # Run every 15 minutes
    catchup=False,
    tags=['dataproc', 'pyspark', 'ephemeral'],
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_ephemeral_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='submit_test_pyspark_job',
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_ephemeral_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done",  # Ensure delete runs even if the job fails
    )

    # Set Task Dependencies
    create_cluster >> submit_pyspark_job >> delete_cluster

