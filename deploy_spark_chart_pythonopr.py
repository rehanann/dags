from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import yaml  # ✅ Required to parse YAML

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='deploy_spark_pythonoperator',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['helm', 'spark'],
)

def deploy_helm_chart():
    url = "http://helm-api-api.default.svc.cluster.local:8000/install"

    # ✅ YAML formatted setValues
    set_values_yaml = """
runAsJob: true
image:
  command: ["/bin/bash"]
  args:
    - "-c"
    - "/opt/spark/bin/spark-submit /opt/spark/work-dir/shared/test2.py"
"""

    # Parse YAML and convert back to string
    parsed = yaml.safe_load(set_values_yaml)
    set_values_str = yaml.dump(parsed, default_flow_style=False)

    payload = {
        "release_name": "switch-values-test",
        "chart_name": "spark-chart/spark-chart",
        "namespace": "gdt",
        "setValues": set_values_str
    }

    payload_str = json.dumps(payload)
    print("Payload string to be sent:")
    print(payload_str)

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    # 🟢 Send request
    response = requests.post(url, data=payload_str, headers=headers)

    # ✅ Print response status and body
    print(f"Response status code: {response.status_code}")
    print("Response body:")
    print(response.text)

    # Raise if error
    response.raise_for_status()

deploy_chart = PythonOperator(
    task_id='deploy_spark_chart',
    python_callable=deploy_helm_chart,
    dag=dag,
)


