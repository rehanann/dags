from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='deploy_spark_chart',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        "release_name": "spark-job",
        "namespace": "gdt",
        "helm_repo_name": "spark-chart",
        "helm_repo_url": "https://rehanann.github.io/spark-chart",  # 🔁 REPLACE this with actual repo URL
        "chart_name": "spark-chart/spark-chart",
        "values_file": "/opt/airflow/helm/values/spark-values.yaml",  # Optional: your custom values file
    },
) as dag:

    deploy_spark = BashOperator(
        task_id='deploy_spark_chart',
        bash_command=(
            "helm repo add {{ params.helm_repo_name }} {{ params.helm_repo_url }} && "
            "helm repo update && "
            "helm upgrade --install {{ params.release_name }} {{ params.chart_name }} "
            "--namespace {{ params.namespace }} "
            "--set image.args='{-c,/opt/spark/bin/spark-submit /opt/spark/work-dir/shared/test2.py}' "
            "--set runAsJob=true"
        ),
    )

    deploy_spark
