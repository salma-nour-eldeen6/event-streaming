from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

KAFKA_JAR = "file:///opt/flink/usrlib/flink-sql-connector-kafka-3.1.0-1.18.jar"


def main():

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()

    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # load kafka connector
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        KAFKA_JAR
    )

    print("Starting Flink Kafka debug job...")

    # Kafka source
    t_env.execute_sql("""
        CREATE TABLE atlas_source (
            fw INT,
            mver STRING,
            dst_addr STRING,
            avg DOUBLE,
            min DOUBLE,
            max DOUBLE,
            sent INT,
            rcvd INT,
            msm_id BIGINT,
            prb_id BIGINT,
            `timestamp` BIGINT,
            `type` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'atlas_measurements',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-atlas-consumer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # print sink
    t_env.execute_sql("""
        CREATE TABLE print_sink (
            fw INT,
            mver STRING,
            dst_addr STRING,
            avg DOUBLE,
            min DOUBLE,
            max DOUBLE,
            sent INT,
            rcvd INT,
            msm_id BIGINT,
            prb_id BIGINT,
            `timestamp` BIGINT,
            `type` STRING
        ) WITH (
            'connector' = 'print'
        )
    """)

    # pipeline
    t_env.execute_sql("""
        INSERT INTO print_sink
        SELECT *
        FROM atlas_source
    """).wait()


if __name__ == "__main__":
    main()