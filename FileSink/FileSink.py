from pyflink.table import (
    DataTypes, TableEnvironment, EnvironmentSettings,
    CsvTableSource, CsvTableSink, WriteMode, TableDescriptor, Schema
)


def main():
    env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    table = TableEnvironment.create(env_settings)

    # all data to one output file
    table.get_config().get_configuration().set_string("parallelism.default", "1")

    table.execute_sql(f"""
        CREATE TABLE countries(
            code STRING,
            name STRING,
            continent STRING,
            wikipedia_link STRING
            ) WITH ( 
            'connector' = 'filesystem',
            'path' = '../countries.csv',
            'format' = 'csv')
        """)

    tableInput = table.from_path('countries')

    print('\nSchema')
    tableInput.print_schema()

    table.create_temporary_table('resultTable', TableDescriptor.for_connector('filesystem')
                       .schema(Schema.new_builder()
                               .column("code", DataTypes.STRING())
                               .column("name", DataTypes.STRING())
                               .column("continent", DataTypes.STRING())
                               .column("wiki", DataTypes.STRING())
                               .build())
                       .option('path', 'result')
                       .option('partition.default-name', 'result')
                       .format('csv')
                       .build())

    tableOutput = table.from_path('resultTable')

    tableOutput.print_schema()

    tableInput.execute_insert('resultTable').wait()

if __name__ == '__main__':
    main()
