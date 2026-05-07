from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import timedelta

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
        bash_command="docker ps",
    )
    create_topic = BashOperator(
    task_id="create_kafka_topic",
    bash_command="bash /opt/airflow/scripts/create_kafka_topic.sh ",
    )
    run_producer = BashOperator(
    task_id="run_kafka_producer",
    bash_command="bash /opt/airflow/scripts/run_kafka_producer.sh ",
    )

    run_bronze = BashOperator(
        task_id="run_bronze_job",
        bash_command="bash /opt/airflow/scripts/run_bronze_job.sh ",
    )

    run_silver = BashOperator(
        task_id="run_silver_job",
        bash_command="bash /opt/airflow/scripts/run_silver_job.sh ",
    )
    wait_for_silver_data = TimeDeltaSensor(
    task_id="wait_for_silver_data",
    delta=timedelta(minutes=30),
    )

    run_gold = BashOperator(
        task_id="run_gold_job",
        bash_command="bash /opt/airflow/scripts/run_gold_job.sh ",
    )

    test_scripts_mount >> test_docker_access
    test_docker_access >> create_topic
    create_topic >> run_bronze
    run_bronze >> run_silver

    run_silver >> run_producer
    run_silver >> wait_for_silver_data
    wait_for_silver_data >> run_gold
        