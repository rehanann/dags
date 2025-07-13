from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='deploy_spark_chart_pythonoperator',  # ✅ DAG name
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['helm', 'spark'],
)

def deploy_helm_chart():
    url = "http://helm-api-api.default.svc.cluster.local:8000/install"
    payload = {
        "release_name": "switch-values-test",
        "chart_name": "spark-chart/spark-chart",
        "namespace": "gdt",
        "values": {
            "runAsJob": True,
            "image": {
                "command": ["/bin/bash"],
                "args": ["-c", "/opt/spark/bin/spark-submit /opt/spark/work-dir/shared/test2.py"]
            }
        }
    }
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()

deploy_chart = PythonOperator(
    task_id='deploy_spark_chart',
    python_callable=deploy_helm_chart,
    dag=dag,
)

