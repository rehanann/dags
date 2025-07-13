import requests
from airflow.operators.python import PythonOperator

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
