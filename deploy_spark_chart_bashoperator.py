from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='deploy_spark_chart_bashoperator',
    default_args=default_args,
    schedule=None,  # ✅ use `schedule`, not `schedule_interval`
    catchup=False,
    tags=['helm', 'spark'],
)

deploy_chart = BashOperator(
    task_id='deploy_spark_chart',
    bash_command="""
    curl -X POST \
      http://helm-api-api.default.svc.cluster.local:8000/install \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '{
        "release_name": "switch-values-test",
        "chart_name": "spark-chart/spark-chart",
        "namespace": "gdt",
        "values": {
          "runAsJob": true,
          "driver": {
            "command": "bash",
            "args": ["-c", "/opt/spark/bin/spark-submit /opt/spark/work-dir/shared/test2.py"]
          }
        }
      }'
    """,
    dag=dag,
)
