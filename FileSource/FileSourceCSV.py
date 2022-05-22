from pyflink.table import (
    DataTypes, TableEnvironment, EnvironmentSettings,
    CsvTableSource
)


def main():
    env_settings = EnvironmentSettings.in_batch_mode()
    table = TableEnvironment.create(env_settings)

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

    print('\nCountries')
    tableInput.print_schema()

    print('\nCountries')
    print(tableInput.to_pandas())



if __name__ == '__main__':
    main()
