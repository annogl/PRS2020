from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

def main():

    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    ds = env.from_collection(
        collection=[(1, 'aaa'), (2, 'bb'), (3, 'c')],
        type_info=Types.ROW([Types.INT(), Types.STRING()]))

    # convert a DataStream to a Table
    table = t_env.from_data_stream(ds)

    print('\ntable data')
    print(table.to_pandas())

if __name__ == '__main__':
    main()