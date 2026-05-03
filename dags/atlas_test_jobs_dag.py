from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="atlas_test_jobs",
    start_date=datetime(2026, 5, 1),
    schedule_interval=None,
    catchup=False,
    tags=["atlas", "test", "flink"],
) as dag:

    test_scripts_mount = BashOperator(
        task_id="test_scripts_mount",
        bash_command="ls -la /opt/airflow/scripts",
    )

    test_docker_access = BashOperator(
        task_id="test_docker_access",
        bash_command="docker ps --format '{{.Names}}'",
    )

    run_bronze = BashOperator(
        task_id="run_bronze_job",
        bash_command="bash /opt/airflow/scripts/run_bronze_job.sh",
    )

    run_silver = BashOperator(
        task_id="run_silver_job",
        bash_command="bash /opt/airflow/scripts/run_silver_job.sh",
    )

    run_gold = BashOperator(
        task_id="run_gold_job",
        bash_command="bash /opt/airflow/scripts/run_gold_job.sh",
    )

    test_scripts_mount >> test_docker_access
    test_docker_access >> [run_bronze, run_silver, run_gold]