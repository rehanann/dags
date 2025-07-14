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

    # ✅ YAML formatted setValues (you can also load this from a file)
    set_values_yaml = """
runAsJob: true
image:
  command: ["/bin/bash"]
  args:
    - "-c"
    - "/opt/spark/bin/spark-submit /opt/spark/work-dir/shared/test2.py"
"""

    # ✅ Parse YAML to dict then dump back to string (cleaned)
    parsed = yaml.safe_load(set_values_yaml)
    set_values_str = yaml.dump(parsed, default_flow_style=False)

    payload = {
        "release_name": "switch-values-test",
        "chart_name": "spark-chart/spark-chart",
        "namespace": "gdt",
        "setValues": set_values_str  # ✅ this is a string
    }

    payload_str = json.dumps(payload)
    print("Payload string to be sent:", payload_str)

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.post(url, data=payload_str, headers=headers)
    response.raise_for_status()

deploy_chart = PythonOperator(
    task_id='deploy_spark_chart',
    python_callable=deploy_helm_chart,
    dag=dag,
)


