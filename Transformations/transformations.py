from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def main():

    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_string("parallelism.default", "1")

    # MAP
    data_stream_1 = env.from_collection(collection=[1, 2, 3, 4, 5],
                                      type_info=Types.INT())
    data_stream_1.map(lambda x: 2 * x, output_type=Types.INT()).print()

    env.execute()

    # FLAT_MAP
    data_stream_2 = env.from_collection(collection=['hello apache flink', 'streaming compute'],
                                      type_info=Types.STRING())
    data_stream_2.flat_map(lambda x: x.split(' '), output_type=Types.STRING()).print()

    env.execute()

    # FILTER
    data_stream_3 = env.from_collection(collection=[0, 1, 2, 3, 4, 5],
                                      type_info=Types.INT())
    data_stream_3.filter(lambda x: x != 0).print()

    env.execute()

    # KEY_BY
    data_stream_4 = env.from_collection(collection=[(1, 'a'), (2, 'a'), (3, 'b')],
                                      type_info=Types.ROW([Types.INT(), Types.STRING()]))
    b = data_stream_4.key_by(lambda x: x[1], key_type=Types.STRING())

    # convert a DataStream to a Table
    table = t_env.from_data_stream(b)
    env.execute()

    print('\ntable data')
    print(table.to_pandas())

    # REDUCE
    data_stream_5 = env.from_collection(collection=[(1, 'a'), (2, 'a'), (3, 'a'), (4, 'b')],
                                      type_info=Types.TUPLE([Types.INT(), Types.STRING()]))
    data_stream_5.key_by(lambda x: x[1]).reduce(lambda a, b: (a[0] + b[0], b[1])).print()

    env.execute()

if __name__ == '__main__':
    main()