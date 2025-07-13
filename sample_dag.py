from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json

def deploy_spark_chart():
    url = "http://helm-api-api.default.svc.cluster.local:8000/install"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }
    payload = {
        "release_name": "mytest",
        "chart_name": "spark-chart/spark-chart",
        "namespace": "test",
        "values": {
            "additionalProp1": {}
        }
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code != 200:
        raise Exception(f"Failed to deploy chart: {response.status_code} {response.text}")
    else:
        print("Deployment response:", response.json())

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="deploy_spark_chart_via_api",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["helm", "spark"],
) as dag:

    deploy_chart = PythonOperator(
        task_id="deploy_spark_chart",
        python_callable=deploy_spark_chart
    )
