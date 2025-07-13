import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.now().subtract(days=1),
}

with DAG(
    dag_id='deploy_spark_chart',
    default_args=default_args,
    schedule=None,  # ← replaced `schedule_interval` with `schedule`
    catchup=False,
    params={
        "release_name": "spark-job",
        "namespace": "gdt",
        "helm_repo_name": "spark-chart",
        "helm_repo_url": "https://rehanann.github.io/spark-chart",
        "chart_name": "spark-chart/spark-chart",
        "values_file": "/opt/airflow/helm/values/spark-values.yaml",
    },
) as dag:

    deploy_spark = BashOperator(
        task_id='deploy_spark_chart',
        bash_command=(
            "helm repo add {{ params.helm_repo_name }} {{ params.helm_repo_url }} && "
            "helm repo update && "
            "helm upgrade --install {{ params.release_name }} {{ params.chart_name }} "
            "--namespace {{ params.namespace }} "
            "--set image.args='[-c,/opt/spark/bin/spark-submit /opt/spark/work-dir/shared/test2.py]' "
            "--set runAsJob=true"
        ),
    )

    deploy_spark