from pyflink.common import JsonRowDeserializationSchema, Types, WatermarkStrategy, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.table import StreamTableEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    env.add_jars("file:/home/faculty/mw/PycharmProjects/PRS2020iuguyfv/flink-sql-connector-kafka-1.15.0.jar")

    type_info = Types.ROW_NAMED(["temp", "moisture", "light", "conductivity", "battery"],
                                [Types.DOUBLE(), Types.DOUBLE(), Types.INT(), Types.INT(),
                                 Types.INT()])
    deserialization_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()

    kafkaSource = FlinkKafkaConsumer(
        topics='xiaomi_json',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': '10.100.6.128:29092',
                    'group.id': 'xiaomi_json'}
    )
    kafkaSource.set_start_from_earliest()

    ds = env.add_source(kafkaSource).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(20)))
    ds.print()

    # convert a DataStream to a Table
    table = t_env.from_data_stream(ds)

    print('\ntable data')
    print(table.print_schema())
    env.execute()


if __name__ == '__main__':
    main()



