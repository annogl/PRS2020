from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSink, OutputFileConfig


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.add_jars("file:///home/marcin/PycharmProjects/FlinkClasses/flink-sql-connector-kafka-1.15.0.jar")

    ds = env.from_collection(
        collection=[(1, 'aaa'), (2, 'bb'), (3, 'c')],
        type_info=Types.ROW([Types.INT(), Types.STRING()]))

    output_path = './output/'
    file_sink = FileSink \
        .for_row_format(output_path, Encoder.simple_string_encoder()) \
        .with_output_file_config(OutputFileConfig.builder().build()) \
        .build()

    ds.sink_to(file_sink)
    ds.print()
    env.execute()


if __name__ == '__main__':
    main()
