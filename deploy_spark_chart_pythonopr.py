from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import yaml

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
    # ✅ Include query params in URL
    installation_name = "switch-values-test"
    namespace = "gdt"

    url = f"http://helm-api-api.default.svc.cluster.local:8000/install?installation={installation_name}&namespace={namespace}"

    # ✅ setValues YAML block
    set_values_yaml = """
runAsJob: true
image:
  command: ["/bin/bash"]
  args:
    - "-c"
    - "/opt/spark/bin/spark-submit /opt/spark/work-dir/shared/test2.py"
"""

    # Clean up formatting (optional)
    parsed_yaml = yaml.safe_load(set_values_yaml)
    set_values_str = yaml.dump(parsed_yaml, default_flow_style=False)

    # ✅ Body structure to match FastAPI's expected schema
    payload = {
        "chart": "spark-chart/spark-chart",
        "releaseName": installation_name,
        "version": "0.2.0",  # You MUST include version per API
        "setValues": set_values_str
    }

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    # ✅ Send the request
    response = requests.post(url, data=json.dumps(payload), headers=headers)

    print(f"Response Status: {response.status_code}")
    print("Response Body:")
    print(response.text)

    response.raise_for_status()

deploy_chart = PythonOperator(
    task_id='deploy_spark_chart',
    python_callable=deploy_helm_chart,
    dag=dag,
)
