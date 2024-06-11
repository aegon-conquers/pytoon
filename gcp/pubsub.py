from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.pubsub import PubSubPullOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import json

# Define default arguments for the DAG
default_args = {
    'start_date': days_ago(1),
    'retries': 1,
}

def check_message_state(**kwargs):
    messages = kwargs['ti'].xcom_pull(task_ids='pull_messages')
    if not messages:
        raise ValueError('No messages received')
    
    for msg in messages:
        payload = json.loads(msg['message']['data'])
        if payload.get('state') == 'success':
            return True
    return False

# Define the DAG
with models.DAG(
    'dataproc_workflow_with_pubsub',
    default_args=default_args,
    schedule_interval=None,  # This DAG will not run on a schedule
) as dag:

    # Task to pull messages from Pub/Sub topic
    pull_messages = PubSubPullOperator(
        task_id='pull_messages',
        project_id='your-project-id',
        subscription='projects/your-project-id/subscriptions/your-subscription',
        max_messages=10,
        ack_messages=True,
    )

    # Task to check if the message state is success
    check_state = PythonOperator(
        task_id='check_state',
        python_callable=check_message_state,
        provide_context=True,
    )

    # Task to create a Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id='your-project-id',
        cluster_name='example-cluster',
        region='your-region',
        cluster_config={
            'master_config': {
                'num_instances': 1,
                'machine_type_uri': 'n1-standard-4',
            },
            'worker_config': {
                'num_instances': 2,
                'machine_type_uri': 'n1-standard-4',
            },
        },
    )

    # Task to submit a PySpark job to the Dataproc cluster
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job',
        job={
            'placement': {
                'cluster_name': 'example-cluster',
            },
            'pyspark_job': {
                'main_python_file_uri': 'gs://your-bucket/path/to/your_script.py',
            },
        },
        region='your-region',
        project_id='your-project-id',
    )

    # Task to delete the Dataproc cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        project_id='your-project-id',
        cluster_name='example-cluster',
        region='your-region',
        trigger_rule=TriggerRule.ALL_DONE,  # Ensure the cluster is deleted even if the job fails
    )

    # Set the task dependencies
    pull_messages >> check_state >> create_cluster >> submit_pyspark_job >> delete_cluster
