from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_string("parallelism.default", "1")
    t_env.get_config().get_configuration().set_string("pipeline.auto-watermark-interval", "800ms")

    env.add_jars("file:///home/marcin/PycharmProjects/FlinkClasses/flink-sql-connector-kafka-1.15.0.jar")

    # nie możemy dodać daty :/
    src_ddl = """
        CREATE TABLE sensor (
            temp DOUBLE,
            moisture DOUBLE,
            light INT,
            conductivity INT,
            battery INT
            ) WITH (
            'connector' = 'kafka',
            'topic' = 'xiaomi_json',
            'properties.bootstrap.servers' = 'localhost:29092',
            'properties.group.id' = 'testGroup',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """

    t_env.execute_sql(src_ddl)

    # create and initiate loading of source Table
    tbl = t_env.from_path('sensor')

    print('\nSchema')
    tbl.print_schema()

    data_stream = t_env.to_data_stream(tbl)

    windowed_rev = data_stream.key_by(lambda a: a[1]).print()
                              # nie mozemy dodac window
                              #.window() # ????
                              #.process() # ????

    env.execute()

if __name__ == '__main__':
    main()



