from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from flink_config import flink_config

#Producer → Kafka → Flink → Iceberg → MinIO

def setup_environment():
    """Setup Flink execution environment"""

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(flink_config.job_parallelism) 

    # Add required JARs
    for jar_path in flink_config.get_jar_paths():
        env.add_jars(jar_path)

    settings = (   #streaming mode 
        EnvironmentSettings
        .new_instance()
        .in_streaming_mode()
        .build()
    )
    # flink work in both stream and batch mode

    t_env = StreamTableEnvironment.create(env, environment_settings=settings) #execution environment + table SQL engine
    t_env.get_config().set("pipeline.jars", ";".join(flink_config.get_jar_paths()))

    # Configure MinIO / Hadoop
    minio_config = flink_config.get_minio_config()
    for key, value in minio_config.items():
        t_env.get_config().set(key, value)

    return env, t_env


def create_source_table(t_env):
    """Create Kafka source table"""
    # Creates a temporary table connected to a Kafka topic
    # Each Kafka message becomes a row in this table
    # Flink can now query this table as if it were a normal database table
    print("Creating Kafka source table...")
    t_env.execute_sql(flink_config.get_kafka_source_ddl())
    print("Kafka source table created")
    # By creating this table table, Flink automatically maps Kafka fields to columns (fw, mver, dst_addr, etc.)


def create_iceberg_catalog_and_table(t_env):
    """Create Iceberg catalog and table"""
    #Creates an Iceberg catalog pointing to the warehouse on MinIO
    print("Creating Iceberg catalog...")

    t_env.execute_sql(flink_config.get_iceberg_catalog_ddl())
    t_env.execute_sql(
        f"USE CATALOG {flink_config.iceberg_catalog_name}"
    )
    t_env.execute_sql(
        f"CREATE DATABASE IF NOT EXISTS {flink_config.iceberg_database}"
    )
    t_env.execute_sql(
        f"USE {flink_config.iceberg_database}"
    )
    print("Creating Iceberg table...")
    t_env.execute_sql(flink_config.get_iceberg_table_ddl())
    print(
        f"Iceberg table {flink_config.iceberg_database}.{flink_config.iceberg_table} ready"
    )


def transform_and_write_data(t_env):
    """Transform Kafka data and write to Iceberg"""
    #Runs a SQL transformation on Kafka data
    print("Starting transformation...")

    processed_table = t_env.sql_query(
        flink_config.get_transform_query()
    )
    t_env.create_temporary_view(
        "processed_measurements",
        processed_table
    )
    result = t_env.execute_sql(
        f"""
        INSERT INTO {flink_config.iceberg_database}.{flink_config.iceberg_table}
        SELECT * FROM processed_measurements
        """
    )

    return result


def run_pipeline():
    """Run the full streaming pipeline"""

    print("Starting Flink Kafka → Iceberg pipeline")

    env, t_env = setup_environment()

    create_source_table(t_env)

    create_iceberg_catalog_and_table(t_env)

    result = transform_and_write_data(t_env)

    print("✓ Streaming job started. Waiting for Kafka data...")

    # Streaming jobs run continuously
    result.wait()


if __name__ == "__main__":
    run_pipeline()