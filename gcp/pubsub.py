import base64
import json
from datetime import datetime
from airflow.models import Variable

def check_message_state(**kwargs):
    messages = kwargs['ti'].xcom_pull(task_ids='pull_messages')
    if not messages:
        raise ValueError('No messages received')
    
    success_found = False
    for msg in messages:
        try:
            # Decode base64 data
            message_data = base64.b64decode(msg['message']['data']).decode('utf-8')
            print(f"Decoded message data: {message_data}")
            
            # Parse JSON
            payload = json.loads(message_data)
            print(f"Parsed payload: {payload}")
            
            # Check for success state and log publish time
            if payload.get('state') == 'success':
                publish_time = msg['message']['publishTime']
                publish_datetime = datetime.strptime(publish_time, '%Y-%m-%dT%H:%M:%S.%fZ')
                print(f"Message published at: {publish_datetime}")
                success_found = True
        except (json.JSONDecodeError, base64.binascii.Error) as e:
            print(f"Error decoding or parsing message: {e}")
            print(f"Message content: {msg['message']['data']}")
    
    if not success_found:
        raise ValueError('No success message found')

# Example usage in the Airflow DAG
with models.DAG(
    'dataproc_workflow_with_pubsub',
    default_args=default_args,
    schedule_interval=None,
) as dag:

    pull_messages = PubSubPullOperator(
        task_id='pull_messages',
        project_id='your-project-id',
        subscription='projects/your-project-id/subscriptions/your-subscription',
        max_messages=10,
        ack_messages=True,
    )

    check_state = PythonOperator(
        task_id='check_state',
        python_callable=check_message_state,
        provide_context=True,
    )

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

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        project_id='your-project-id',
        cluster_name='example-cluster',
        region='your-region',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    pull_messages >> check_state >> create_cluster >> submit_pyspark_job >> delete_cluster
