from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Tumble, Slide


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_string("parallelism.default", "1")
    t_env.get_config().get_configuration().set_string("pipeline.auto-watermark-interval", "800ms")


    env.add_jars("file:///home/marcin/PycharmProjects/FlinkClasses/flink-sql-connector-kafka-1.15.0.jar")

    src_ddl = """
        CREATE TABLE sensor (
            event_time TIMESTAMP(3) METADATA FROM 'timestamp',
            WATERMARK FOR event_time AS event_time,
            temp DOUBLE,
            moisture DOUBLE,
            light INT,
            conductivity INT,
            battery INT,
            proctime AS PROCTIME()
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

    # tbl.execute().print()

    windowed_rev = tbl.window(Slide.over(lit(10).minutes) \
                              .every(lit(5).minutes)
                              .on(tbl.event_time)
                              .alias('w')) \
        .group_by(col('w'), tbl.temp, tbl.proctime) \
        .select(tbl.temp,
                tbl.proctime,
                col('w').start.alias('window_start'),
                col('w').end.alias('window_end'))


    print('\nSchema')
    windowed_rev.print_schema()

    windowed_rev.execute().print()

if __name__ == '__main__':
    main()



